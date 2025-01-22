use bytes::Bytes;

#[test]
fn test_01() {
    let v = vec![65_u8];
    let b: Bytes = v.into();
    let kk: &[u8] = b.as_ref();
    println!("{:?} {:?}", kk[0], kk.len());
    println!("{:?}", b);
}
