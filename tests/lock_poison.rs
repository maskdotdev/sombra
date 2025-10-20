use sombra::error::{acquire_lock, GraphError};
use std::sync::{Arc, Mutex};
use std::thread;

#[test]
fn poisoned_mutex_returns_corruption_error() {
    let lock = Arc::new(Mutex::new(()));
    let lock_clone = lock.clone();

    let handle = thread::spawn(move || {
        let _guard = lock_clone.lock().unwrap();
        panic!("intentional panic to poison mutex");
    });

    assert!(handle.join().is_err());

    let err = acquire_lock(lock.as_ref()).expect_err("poisoned mutex should error");
    match err {
        GraphError::Corruption(message) => {
            assert!(
                message.contains("Database lock poisoned"),
                "unexpected corruption message: {message}"
            );
        }
        other => panic!("expected GraphError::Corruption, got {other:?}"),
    }
}
