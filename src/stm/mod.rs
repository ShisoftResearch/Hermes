use parking_lot::Mutex;
use parking_lot::RwLock;
use std::any::Any;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::mem;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;

type UnsafeValCell = UnsafeCell<Arc<Any + Send + Sync>>;

struct TxnVal {
    id: usize,
    read: usize,  // last read id
    write: usize, // last write id
    version: usize,
    owner: usize,
    data: UnsafeValCell,
}

fn unsafe_val_from<T>(val: T) -> UnsafeValCell
where
    T: Any + Send + Sync + Clone,
{
    UnsafeCell::new(Arc::new(val))
}

impl TxnVal {
    fn new<T>(id: usize, val: T) -> TxnVal
    where
        T: Any + Send + Sync + Clone,
    {
        TxnVal {
            id,
            data: unsafe_val_from(val),
            read: 0,
            write: 0,
            version: 1,
            owner: 0,
        }
    }

    unsafe fn get<T>(&self) -> Arc<T>
    where
        T: Any + Send + Sync + Clone,
    {
        let data = &*self.data.get();
        data.clone()
            .downcast::<T>()
            .expect("wrong type for txn val")
    }
}

impl Default for TxnValRef {
    fn default() -> Self {
        TxnValRef { id: 0 }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TxnValRef {
    id: usize,
}

struct TxnManagerInner {
    val_counter: AtomicUsize,
    txn_counter: AtomicUsize,
    states: RwLock<HashMap<usize, Arc<Mutex<TxnVal>>>>,
}

pub struct TxnManager {
    inner: Arc<TxnManagerInner>,
}

impl TxnManager {
    pub fn new() -> TxnManager {
        TxnManager {
            inner: Arc::new(TxnManagerInner {
                val_counter: AtomicUsize::new(1),
                txn_counter: AtomicUsize::new(1),
                states: RwLock::new(HashMap::new()),
            }),
        }
    }
    pub fn with_value<T>(&self, val: T) -> TxnValRef
    where
        T: Any + Send + Sync + Clone,
    {
        let val_id = self.inner.next_val_id();
        let val = TxnVal::new(val_id, val);
        let mut states = self.inner.states.write();
        states.insert(val_id, Arc::new(Mutex::new(val)));
        TxnValRef { id: val_id }
    }
    fn transaction_optional_commit<R, B>(
        &self,
        block: B,
        auto_commit: bool,
    ) -> Result<(Txn, R), TxnErr>
    where
        B: Fn(&mut Txn) -> Result<R, TxnErr>,
    {
        loop {
            let txn_id = self.inner.txn_counter.fetch_add(1, Ordering::Relaxed);
            let mut txn = Txn::new(&self.inner, txn_id);
            match block(&mut txn) {
                Ok(res) => match txn.prepare() {
                    Ok(_) => {
                        trace!("Prepared {}", txn_id);
                        if auto_commit {
                            txn.commit();
                        }
                        return Ok((txn, res));
                    }
                    Err(TxnErr::Aborted) => {
                        trace!("Prepare aborted {}", txn_id);
                        return Err(txn.abort().err().unwrap());
                    }
                    Err(TxnErr::NotRealizable) => {
                        trace!("Prepare not realizable {}", txn_id);
                        txn.abort().err().unwrap();
                        continue;
                    }
                },
                Err(TxnErr::Aborted) => return Err(TxnErr::Aborted),
                Err(TxnErr::NotRealizable) => {
                    txn.abort().err().unwrap();
                    continue;
                }
            }
        }
    }
    pub fn transaction<R, B>(&self, block: B) -> Result<R, TxnErr>
    where
        B: Fn(&mut Txn) -> Result<R, TxnErr>,
    {
        self.transaction_optional_commit(block, true)
            .map(|(_, r)| r)
    }
}

impl TxnManagerInner {
    fn get_state(&self, id: &usize) -> Option<Arc<Mutex<TxnVal>>> {
        let states = self.states.read();
        return states.get(id).map(|state| state.clone());
    }
    fn next_val_id(&self) -> usize {
        self.val_counter.fetch_add(1, Ordering::Relaxed)
    }
}

struct DataObject {
    changed: bool,
    new: bool,
    data: Option<UnsafeValCell>,
}

struct HistoryEntry {
    id: usize,
    version: usize,
    data: Option<UnsafeValCell>,
    op: HistoryOp,
}

enum HistoryOp {
    Delete,
    Create,
    Update,
}

pub struct Txn {
    manager: Arc<TxnManagerInner>,
    values: HashMap<usize, DataObject>,
    state: TxnState,
    history: Vec<HistoryEntry>,
    defers: HashMap<usize, Box<Fn()>>,
    id: usize,
}

impl Txn {
    fn new(manager: &Arc<TxnManagerInner>, id: usize) -> Txn {
        Txn {
            manager: manager.clone(),
            values: HashMap::new(),
            state: TxnState::Started,
            history: Vec::new(),
            defers: HashMap::new(),
            id,
        }
    }

    // Discuss on Read/Write ordering
    // Each read and write operations have transaction ids represents ordering
    // Suppose Transaction id A < B
    // Read for A is R_a, write is W_a. Read for B is R_b, write W_b
    // For  W_a and R_a, read value from A write cache
    //      R_a and R_a, not need for special treatment
    //      W_b and R_a, B should be rejected on prepare
    //      R_b and R_a, need to record id for B (the latest)
    //
    //      W_a and R_b, B should wait for A to finish
    //      R_a and R_b, not need for special treatment
    //      W_b and R_b, B should read from its cache
    //      R_a and R_b, need to record id for B (the latest)
    pub fn read<T>(&mut self, val_ref: TxnValRef) -> Result<Option<Arc<T>>, TxnErr>
    where
        T: 'static + Send + Sync + Clone,
    {
        assert_eq!(self.state, TxnState::Started);
        let val_id = val_ref.id;
        // W_a and R_a
        if let Some(v) = self.values.get(&val_id) {
            return Ok(unsafe {
                v.data
                    .as_ref()
                    .map(|v| (&*v.get()).clone().downcast::<T>().expect(""))
            });
        }
        if let Some(state_lock) = self.manager.get_state(&val_id) {
            let mut state = state_lock.lock();
            {
                let state_owner = state.owner;
                let val_last_write = &mut state.read;
                if *val_last_write > self.id || state_owner != 0 {
                    // cannot read when the transactions happens after last write
                    trace!(
                        "Not realizable on read {} due to read too late {}/{}",
                        val_id,
                        val_last_write,
                        self.id
                    );
                    return Err(TxnErr::NotRealizable);
                }
            }
            {
                let val_last_read = &mut state.read;
                if *val_last_read < self.id {
                    // set last read to reject earlier writer
                    *val_last_read = self.id;
                }
            }
            let value: Arc<T> = unsafe { state.get() };
            self.values.insert(
                val_id,
                DataObject {
                    changed: false,
                    new: false,
                    data: Some(UnsafeCell::new(value.clone())),
                },
            );
            return Ok(Some(value));
        } else {
            return Ok(None);
        }
    }

    pub fn read_owned<T>(&mut self, val_ref: TxnValRef) -> Result<Option<T>, TxnErr>
    where
        T: 'static + Send + Sync + Clone,
    {
        self.read(val_ref)
            .map(|opt: Option<Arc<T>>| opt.map(|arc| (*arc).clone()))
    }

    // Write to transaction cache for commit
    pub fn update<T>(&mut self, val_ref: TxnValRef, value: T)
    where
        T: 'static + Send + Sync + Clone,
    {
        assert_eq!(self.state, TxnState::Started);
        let val_id = val_ref.id;
        if let Some(ref mut data_obj) = self.values.get_mut(&val_id) {
            if data_obj.data.is_none() {
                data_obj.data = Some(unsafe_val_from(value));
            } else {
                data_obj
                    .data
                    .as_ref()
                    .map(|data| unsafe { *(&mut *data.get()) = Arc::new(value) });
            }
            data_obj.changed = true;
            return;
        }
        self.values.insert(
            val_id,
            DataObject {
                data: Some(unsafe_val_from(value)),
                changed: true,
                new: false,
            },
        );
    }

    pub fn new_value<T>(&mut self, value: T) -> TxnValRef
    where
        T: 'static + Send + Sync + Clone,
    {
        assert_eq!(self.state, TxnState::Started);
        let val_id = self.manager.next_val_id();
        self.values.insert(
            val_id,
            DataObject {
                data: Some(unsafe_val_from(value)),
                changed: true,
                new: true,
            },
        );
        return TxnValRef { id: val_id };
    }

    pub fn delete(&mut self, val_ref: TxnValRef) -> Option<()> {
        let val_id = val_ref.id;
        if let Some(ref mut v) = self.values.get_mut(&val_id) {
            if v.data.is_some() {
                // set writing value
                v.data = None;
                v.changed = true;
                v.new = false;
                return Some(());
            } else {
                // already deleted
                return None;
            }
        }
        trace!("read locking manager state on delete");
        if self.manager.states.read().get(&val_id).is_some() {
            // found the value but does not in the
            self.values.insert(
                val_id,
                DataObject {
                    changed: true,
                    new: false,
                    data: None,
                },
            );
            Some(())
        } else {
            None
        }
    }

    // prepare checks if realizable and obtain locks on values to ensure no read and write on them,
    // and then perform write operations
    pub fn prepare(&mut self) -> Result<(), TxnErr> {
        trace!("Preparing {}", self.id);
        assert_eq!(self.state, TxnState::Started);
        // obtain all existing value locks
        let mut value_locks = vec![];
        let mut value_guards = HashMap::new();
        let history = &mut self.history;
        let txn_id = self.id;
        trace!("obtaining locks {}", self.id);
        for (id, obj) in &self.values {
            if !obj.changed {
                continue;
            } // skip readonly lock
            if let Some(ref val) = self.manager.get_state(id) {
                value_locks.push(val.clone());
            }
        }
        trace!("obtaining guards {}", self.id);
        for lock in &value_locks {
            let guard = lock.lock();
            if guard.read > txn_id || guard.write > txn_id || guard.owner != 0 {
                trace!(
                    "Not realizable on prepare due to r/w txn id and owner check: {}/{}, owner {}",
                    guard.read,
                    guard.write,
                    guard.owner
                );
                return Err(TxnErr::NotRealizable);
            }
            value_guards.insert(guard.id, guard);
        }
        trace!("checking and updating data {}", self.id);
        for (id, obj) in &mut self.values {
            if !obj.changed {
                trace!("ignore read data {}", id);
                continue;
            } // ignore read
            if obj.new {
                trace!("Creating data {}", id);
                trace!(
                    "locking manager state prepare create data {} txn: {}",
                    id,
                    txn_id
                );
                let mut states = self.manager.states.write();
                if states.contains_key(id) {
                    trace!(
                        "Not realizable due to creating existed value for id: {}",
                        id
                    );
                    return Err(TxnErr::NotRealizable);
                }
                if let Some(ref mut new_data) = &mut obj.data {
                    let new_val_owned = mem::replace(new_data, unsafe_val_from(()));
                    states.insert(
                        *id,
                        Arc::new(Mutex::new(TxnVal {
                            id: *id,
                            data: new_val_owned,
                            read: txn_id,
                            write: txn_id,
                            version: 1,
                            owner: txn_id,
                        })),
                    );
                    history.push(HistoryEntry {
                        id: *id,
                        data: None,
                        version: 1,
                        op: HistoryOp::Create,
                    });
                    continue;
                } else {
                    unreachable!()
                }
            }
            // following part assumes value exists in txn manager but need to be changed
            if value_guards.contains_key(id) {
                if let (Some(ref mut new_obj), Some(ref mut val)) =
                    (&mut obj.data, value_guards.get_mut(id))
                {
                    trace!("Updating data {}", id);
                    let new_val_owned = mem::replace(new_obj, unsafe_val_from(()));
                    let old_val_owned = mem::replace(&mut val.data, new_val_owned);
                    val.version += 1;
                    val.owner = txn_id;
                    history.push(HistoryEntry {
                        id: *id,
                        data: Some(old_val_owned),
                        version: val.version,
                        op: HistoryOp::Update,
                    });
                    continue;
                }
                // Delete
                {
                    trace!(
                        "locking manager state prepare delete data {} txn: {}",
                        id,
                        txn_id
                    );
                    let mut states = self.manager.states.write();
                    if let Some(_) = states.remove(id) {
                        trace!("Deleting data {}", id);
                        let mut txn_val = value_guards.get_mut(id).unwrap();
                        let removed_val_owned =
                            mem::replace(&mut txn_val.data, unsafe_val_from(()));
                        history.push(HistoryEntry {
                            id: *id,
                            data: Some(removed_val_owned),
                            version: txn_val.version,
                            op: HistoryOp::Delete,
                        });
                        continue;
                    }
                }
            }
            // success operations should have been continued already
            return Err(TxnErr::NotRealizable);
        }
        self.state = TxnState::Prepared;
        return Ok(());
    }

    pub fn abort(&mut self) -> Result<(), TxnErr> {
        if self.state == TxnState::Aborted {
            return Ok(());
        }
        {
            let txn_id = self.id;
            trace!("locking manager state on abort {}", txn_id);
            let mut states = self.manager.states.write();
            for history in &mut self.history {
                let id = history.id;
                match history.op {
                    HistoryOp::Create => {
                        if let Some(removed) = states.remove(&id) {
                            // validate removed created
                            if removed.lock().version != 1 {
                                // removed changed value, put it back
                                states.insert(id, removed);
                            }
                        }
                    }
                    HistoryOp::Delete => {
                        if !states.contains_key(&id) {
                            let owned_removed = if let Some(ref mut val) = history.data {
                                mem::replace(val, unsafe_val_from(()))
                            } else {
                                unreachable!()
                            };
                            states.insert(
                                id,
                                Arc::new(Mutex::new(TxnVal {
                                    id,
                                    read: txn_id,
                                    write: txn_id,
                                    version: history.version,
                                    data: owned_removed,
                                    owner: 0,
                                })),
                            );
                        }
                    }
                    HistoryOp::Update => {
                        if let Some(ref mut val) = states.get(&id) {
                            let mut val_guard = val.lock();
                            if val_guard.version == history.version {
                                let owned_old = if let Some(ref mut val) = history.data {
                                    mem::replace(val, unsafe_val_from(()))
                                } else {
                                    unreachable!()
                                };
                                val_guard.version -= 1;
                                val_guard.data = owned_old;
                            }
                        }
                    }
                }
            }
        }
        self.state = TxnState::Aborted;
        self.end();
        return Err(TxnErr::Aborted);
    }

    // run certain function when the transaction succeed
    // id is used to identify the function to prevent double spend
    pub fn defer<F>(&mut self, id: usize, func: F)
    where
        F: Fn() + 'static,
    {
        self.defers.insert(id, Box::new(func));
    }

    pub fn commit(&mut self) {
        self.defers.iter().for_each(|(_, func)| (func)());
        self.end();
    }

    // cleanup all locks and release resource to other threads
    fn end(&mut self) {
        let txn_id = self.id;
        if self.state == TxnState::Aborted || self.state == TxnState::Prepared {
            trace!("locking manager state on end: {}", txn_id);
            let states = self.manager.states.read();
            for (id, _) in &self.values {
                if let Some(lock) = states.get(id) {
                    let mut val = lock.lock();
                    if val.owner == txn_id {
                        val.owner = 0;
                    }
                }
            }
            if self.state == TxnState::Prepared {
                self.state = TxnState::Committed
            } else {
                self.state = TxnState::Ended
            }
        }
    }
}

#[derive(Debug)]
pub enum TxnErr {
    Aborted,
    NotRealizable,
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum TxnState {
    Started,
    Aborted,
    Prepared,
    Committed,
    Ended,
}

#[cfg(test)]
mod test {
    use super::*;
    use simple_logger;
    use std::thread;

    #[test]
    fn single_op() {
        simple_logger::init();
        let manager = TxnManager::new();
        let val = manager
            .transaction(|txn| {
                // C
                Ok(txn.new_value::<u32>(123))
            })
            .unwrap();
        manager.transaction(|txn| {
            // R
            assert_eq!(txn.read_owned::<u32>(val)?.unwrap(), 123);
            Ok(())
        });
        manager.transaction(|txn| {
            assert_eq!(txn.read_owned::<u32>(val)?.unwrap(), 123);
            Ok(())
        });
        manager.transaction(|txn| {
            // U
            txn.update::<u32>(val, 456);
            Ok(())
        });
        manager.transaction(|txn| {
            assert_eq!(txn.read_owned::<u32>(val)?.unwrap(), 456);
            Ok(())
        });
        manager.transaction(|txn| {
            // D
            txn.delete(val);
            Ok(())
        });
        manager.transaction(|txn| {
            assert!(txn.read::<u32>(val)?.is_none());
            Ok(())
        });
    }

    #[test]
    fn write_reads() {
        simple_logger::init();
        let manager = TxnManager::new();
        let v1 = manager.with_value::<u64>(123);
        let v2 = manager
            .transaction(|txn| {
                assert_eq!(txn.read_owned::<u64>(v1)?.unwrap(), 123);
                txn.update::<u64>(v1, 321);
                assert_eq!(txn.read_owned::<u64>(v1)?.unwrap(), 321);
                let v1_val = txn.read_owned::<u64>(v1)?.unwrap();
                Ok(txn.new_value(v1_val))
            })
            .unwrap();
        manager.transaction(|txn| {
            assert_eq!(txn.read_owned::<u64>(v2)?.unwrap(), 321);
            Ok(())
        });
    }

    #[test]
    fn parallel_counter() {
        simple_logger::init();
        let manager = Arc::new(TxnManager::new());
        let thread_count = 50;
        let v1 = manager.with_value(0);
        let mut threads = vec![];
        for _ in 0..thread_count {
            let manager = manager.clone();
            let th = thread::spawn(move || {
                manager.transaction(|txn| {
                    let mut v = txn.read_owned::<i32>(v1)?.unwrap();
                    v += 1;
                    txn.update(v1, v);
                    Ok(())
                });
            });
            threads.push(th);
        }
        for th in threads {
            th.join().unwrap();
        }
        assert_eq!(
            manager
                .transaction(|txn| txn.read_owned::<i32>(v1).map(|opt| opt.unwrap()))
                .unwrap(),
            thread_count
        );
    }
}
