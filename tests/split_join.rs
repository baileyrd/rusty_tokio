use rusty_tokio::io::{self, duplex, AsyncReadExt, AsyncWriteExt};
use rusty_tokio::Runtime;

#[test]
fn split_read_half_and_write_half_work_concurrently() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, mut b) = duplex(64);
        let (mut read_half, mut write_half) = io::split(a);

        let writer = rusty_tokio::spawn(async move {
            write_half.write_all(b"from write half").await.unwrap();
        });
        let reader = rusty_tokio::spawn(async move {
            let mut buf = [0u8; 4];
            read_half.read_exact(&mut buf).await.unwrap();
            buf
        });

        b.write_all(b"pong").await.unwrap();
        writer.await.unwrap();

        let buf = reader.await.unwrap();
        assert_eq!(&buf, b"pong");

        let mut received = [0u8; 15];
        b.read_exact(&mut received).await.unwrap();
        assert_eq!(&received, b"from write half");
    });
}

#[test]
fn unsplit_recombines_into_the_original_value() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, mut b) = duplex(64);
        let (mut read_half, write_half) = io::split(a);

        b.write_all(b"world").await.unwrap();
        let mut buf = [0u8; 5];
        read_half.read_exact(&mut buf).await.unwrap();
        assert_eq!(&buf, b"world");

        let mut a = read_half.unsplit(write_half);
        // The recombined value is still the same live stream -- further
        // I/O through it still works.
        a.write_all(b"!").await.unwrap();
        let mut tail = [0u8; 1];
        b.read_exact(&mut tail).await.unwrap();
        assert_eq!(&tail, b"!");
    });
}

#[test]
#[should_panic(expected = "didn't come from the same split")]
fn unsplit_panics_across_different_split_calls() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (a, _b) = duplex(64);
        let (c, _d) = duplex(64);
        let (read_half_a, _write_half_a) = io::split(a);
        let (_read_half_c, write_half_c) = io::split(c);
        let _ = read_half_a.unsplit(write_half_c);
    });
}

#[test]
fn join_combines_an_independent_reader_and_writer() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        // Pipe 1 feeds `joined`'s read side; pipe 2 receives whatever
        // `joined` writes -- the two are otherwise unrelated.
        let (mut input_writer, input_reader) = duplex(64);
        let (output_writer, mut output_reader) = duplex(64);

        input_writer.write_all(b"joined input").await.unwrap();
        input_writer.shutdown().await.unwrap();

        let mut joined = io::join(input_reader, output_writer);

        let mut received = Vec::new();
        joined.read_to_end(&mut received).await.unwrap();
        assert_eq!(received, b"joined input");

        joined.write_all(b"joined output").await.unwrap();
        joined.shutdown().await.unwrap();
        drop(joined);

        let mut sink_received = Vec::new();
        output_reader.read_to_end(&mut sink_received).await.unwrap();
        assert_eq!(sink_received, b"joined output");
    });
}

#[test]
fn join_get_ref_get_mut_into_inner_reach_both_halves() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (_a, reader) = duplex(64);
        let (writer, _b) = duplex(64);
        let mut joined = io::join(reader, writer);

        let _ = joined.get_ref();
        let _ = joined.get_mut();
        let (_reader, _writer) = joined.into_inner();
    });
}
