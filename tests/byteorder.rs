use rusty_tokio::io::{self, AsyncReadExt, AsyncWriteExt};
use rusty_tokio::Runtime;

macro_rules! roundtrip_test {
    ($test_name:ident, $write_be:ident, $read_be:ident, $write_le:ident, $read_le:ident, $ty:ty, $value:expr) => {
        #[test]
        fn $test_name() {
            let rt = Runtime::new().unwrap();
            rt.block_on(async {
                let value: $ty = $value;

                let (mut a, mut b) = io::duplex(64);
                a.$write_be(value).await.unwrap();
                let got = b.$read_be().await.unwrap();
                assert_eq!(got, value);

                let (mut a, mut b) = io::duplex(64);
                a.$write_le(value).await.unwrap();
                let got = b.$read_le().await.unwrap();
                assert_eq!(got, value);
            });
        }
    };
}

#[test]
fn u8_i8_roundtrip() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(64);
        a.write_u8(200).await.unwrap();
        assert_eq!(b.read_u8().await.unwrap(), 200);

        let (mut a, mut b) = io::duplex(64);
        a.write_i8(-100).await.unwrap();
        assert_eq!(b.read_i8().await.unwrap(), -100);
    });
}

roundtrip_test!(
    u16_roundtrip,
    write_u16,
    read_u16,
    write_u16_le,
    read_u16_le,
    u16,
    0xABCD
);
roundtrip_test!(
    i16_roundtrip,
    write_i16,
    read_i16,
    write_i16_le,
    read_i16_le,
    i16,
    -12345
);
roundtrip_test!(
    u32_roundtrip,
    write_u32,
    read_u32,
    write_u32_le,
    read_u32_le,
    u32,
    0xDEADBEEF
);
roundtrip_test!(
    i32_roundtrip,
    write_i32,
    read_i32,
    write_i32_le,
    read_i32_le,
    i32,
    -123_456_789
);
roundtrip_test!(
    u64_roundtrip,
    write_u64,
    read_u64,
    write_u64_le,
    read_u64_le,
    u64,
    0xDEADBEEFCAFEBABE
);
roundtrip_test!(
    i64_roundtrip,
    write_i64,
    read_i64,
    write_i64_le,
    read_i64_le,
    i64,
    -123_456_789_012_345
);
roundtrip_test!(
    u128_roundtrip,
    write_u128,
    read_u128,
    write_u128_le,
    read_u128_le,
    u128,
    0xDEADBEEFCAFEBABE_0123456789ABCDEF
);
roundtrip_test!(
    i128_roundtrip,
    write_i128,
    read_i128,
    write_i128_le,
    read_i128_le,
    i128,
    -123_456_789_012_345_678_901
);
roundtrip_test!(
    f32_roundtrip,
    write_f32,
    read_f32,
    write_f32_le,
    read_f32_le,
    f32,
    std::f32::consts::PI
);
roundtrip_test!(
    f64_roundtrip,
    write_f64,
    read_f64,
    write_f64_le,
    read_f64_le,
    f64,
    std::f64::consts::E
);

#[test]
fn big_endian_bytes_match_the_documented_wire_format() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(64);
        a.write_u32(0x01020304).await.unwrap();
        let mut raw = [0u8; 4];
        b.read_exact(&mut raw).await.unwrap();
        assert_eq!(raw, [0x01, 0x02, 0x03, 0x04]);
    });
}

#[test]
fn little_endian_bytes_match_the_documented_wire_format() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
        let (mut a, mut b) = io::duplex(64);
        a.write_u32_le(0x01020304).await.unwrap();
        let mut raw = [0u8; 4];
        b.read_exact(&mut raw).await.unwrap();
        assert_eq!(raw, [0x04, 0x03, 0x02, 0x01]);
    });
}
