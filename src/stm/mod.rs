use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use parking_lot::Mutex;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::any::Any;
use std::borrow::BorrowMut;
use std::sync::atomic::Ordering;
use std::mem;
use parking_lot::RwLock;

type UnsafeValCell = UnsafeCell<Arc<Any + Send + Sync>>;

struct TxnVal {
    id: usize,
    read: usize, // last read id
    write: usize, // last write id
    owner: usize, // the lock, 0 for released
    data: UnsafeValCell
}

fn unsafe_val_from<T>(val: T) -> UnsafeValCell where T: Any + Send + Sync {
    UnsafeCell::new(Arc::new(val))
}

impl TxnVal {
    fn new<T>(id: usize, val: T) -> TxnVal where T: Any + Send + Sync {
        TxnVal {
            id, data: unsafe_val_from(val),
            read: 0, write: 0, owner: 0
        }
    }

    unsafe fn get<T>(&self) -> Arc<T> where T: Any + Send + Sync {
        let data = &*self.data.get();
        data.clone().downcast::<T>().expect("wrong type for txn val")
    }
    unsafe fn set<T>(&self, val: T) -> Arc<T>  where T: Any + Send + Sync {
        mem::replace(&mut *self.data.get(), Arc::new(val))
            .downcast::<T>()
            .expect("wrong type for replacing txn val")
    }
}

#[derive(Clone, Copy)]
pub struct TxnValRef {
    id: usize
}

struct TxnManagerInner {
    val_counter: AtomicUsize,
    txn_counter: AtomicUsize,
    states: RwLock<BTreeMap<usize, Arc<Mutex<TxnVal>>>>
}

pub struct TxnManager {
    inner: Arc<TxnManagerInner>
}

impl TxnManager {
    pub fn new() -> TxnManager {
        TxnManager {
            inner: Arc::new(TxnManagerInner {
                val_counter: AtomicUsize::new(1),
                txn_counter: AtomicUsize::new(1),
                states: RwLock::new(BTreeMap::new())
            })
        }
    }
    pub fn with_value<T>(&self, val: T) -> TxnValRef where T: Any + Send + Sync {
        let val_id = self.inner.next_val_id();
        let val = TxnVal::new(val_id, val);
        let mut states = self.inner.states.write();
        states.insert(val_id, Arc::new(Mutex::new(val)));
        TxnValRef { id: val_id }
    }
    pub fn transaction<R, B>(&self, block: B)
        -> Result<R, TxnErr> where B: Fn(&mut Txn) -> Result<R, TxnErr>
    {
        loop {
            let txn_id = self.inner.txn_counter.fetch_add(1, Ordering::Relaxed);
            let mut txn = Txn::new(&self.inner, txn_id);
            let result = block(&mut txn);
            if let Ok(ret) = result {

            }
        }
        unimplemented!();
    }
}

impl TxnManagerInner {
    fn get_state(&self, id: usize) -> Option<Arc<Mutex<TxnVal>>> {
        let states = self.states.read();
        return states
            .get(&id)
            .map(|state| state.clone());
    }
    fn next_val_id(&self) -> usize {
        self.val_counter.fetch_add(1, Ordering::Relaxed)
    }
}

struct DataObject {
    changed: bool,
    new: bool,
    data: Option<UnsafeValCell>
}

pub struct Txn {
    manager: Arc<TxnManagerInner>,
    values: BTreeMap<usize, DataObject>,
    state: TxnState,
    id: usize
}

impl Txn {
    fn new(manager: &Arc<TxnManagerInner>, id: usize) -> Txn {
        Txn {
            manager: manager.clone(),
            values: BTreeMap::new(),
            state: TxnState::Started,
            id
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
    pub fn read<T>(&mut self, val_ref: TxnValRef) -> Result<Option<Arc<T>>, TxnErr> where T: 'static + Send + Sync {
        assert_eq!(self.state, TxnState::Started);
        let val_id = val_ref.id;
        // W_a and R_a
        if let Some(v) = self.values.get(&val_id) {
            return Ok(unsafe {
                v.data.as_ref().map(|v| (&* v.get()).clone().downcast::<T>().expect(""))
            })
        }
        if let Some(state_lock) = self.manager.get_state(val_id) {
            let mut state = state_lock.lock();
            {
                let val_last_write = &mut state.read;
                if *val_last_write > self.id {
                    // cannot read when the transactions happens after last write
                    return Err(TxnErr::NotRealizable)
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
            self.values.insert(val_id, DataObject {
                changed: false, new: false, data: Some(UnsafeCell::new(value.clone()))
            });
            return Ok(Some(value))
        } else {
            return Ok(None);
        }
    }

    // Write to transaction cache for commit
    pub fn write<T>(&mut self, val_ref: TxnValRef, value: T) where T: 'static + Send + Sync {
        assert_eq!(self.state, TxnState::Started);
        let val_id = val_ref.id;
        if let Some(ref mut data_obj) = self.values.get_mut(&val_id) {
            if data_obj.data.is_none() {
                data_obj.data = Some(unsafe_val_from(value));
            } else {
                data_obj.data.as_ref().map(|data| {
                    unsafe { *(&mut *data.get()) = Arc::new(value) }
                });
            }
            data_obj.changed = true;
            return;
        }
        self.values.insert(val_id, DataObject {
            data: Some(unsafe_val_from(value)),
            changed: true,
            new: !self.manager.states.read().contains_key(&val_id)
        });
    }

    pub fn new_value<T>(&mut self, value: T) -> TxnValRef where T: 'static + Send + Sync {
        assert_eq!(self.state, TxnState::Started);
        let val_id = self.manager.next_val_id();
        self.values.insert(val_id, DataObject {
            data: Some(unsafe_val_from(value)),
            changed: true,
            new: true
        });
        return TxnValRef { id: val_id }
    }

    pub fn delete(&mut self, val_ref: TxnValRef) -> Option<()> {
        let val_id = val_ref.id;
        if let Some(ref mut v) = self.values.get_mut(&val_id) {
            if v.data.is_some() {
                // set writing value
                v.data = None;
                return Some(());
            } else {
                // already deleted
                return None;
            }
        }
        if self.manager.states.read().get(&val_id).is_some() {
            // found the value but does not in the
            self.values.insert(val_id, DataObject {
                changed: true, new: false, data: None
            });
            Some(())
        } else {
            None
        }
    }

    // prepare checks if realizable and obtain locks on values to ensure no read and write on them,
    // and then perform write operations
    pub fn prepare(&mut self) -> Result<(), TxnErr> {
        assert_eq!(self.state, TxnState::Started);
        // obtain all value locks
        for (id, val) in &self.values {

        }
        unimplemented!();
    }

    pub fn abort(&mut self) -> Result<(), TxnErr> {
        match self.state {
            TxnState::Started => {
                // Do nothing
            },
            TxnState::Prepared => {

            },
            TxnState::Committed => {},
            TxnState::Aborted => unreachable!()
        }
        self.state = TxnState::Aborted;
        return Err(TxnErr::Aborted);
    }


    // cleanup all locks and release resource to other threads
    fn end(&self) {
        match self.state {
            TxnState::Aborted => {},
            TxnState::Committed => {},
            _ => panic!("State should either be aborted or committed to end")
        }
    }
}

pub enum TxnErr {
    Aborted,
    TooManyRetry,
    NotRealizable
}

#[derive(Clone, Eq, PartialEq, Debug)]
enum TxnState {
    Started,
    Aborted,
    Prepared,
    Committed
}