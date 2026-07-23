use rusty_tokio::pin;
use rusty_tokio::Runtime;

#[test]
fn pin_re_export_stack_pins_a_future_that_can_then_be_awaited() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let future = async { 41 + 1 };
        let future = pin!(future);
        let value = future.await;
        assert_eq!(value, 42);
    });
}

#[test]
fn pin_re_export_is_the_same_macro_as_std_pin_pin() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let a = std::pin::pin!(async { 1 });
        let b = pin!(async { 2 });
        assert_eq!(a.await, 1);
        assert_eq!(b.await, 2);
    });
}
