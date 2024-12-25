use bytes::Bytes;

#[test]
fn test_01() {
    let mut v = Vec::new();
    v.push(65 as u8);
    let b: Bytes = v.into();
    let kk: &[u8] = b.as_ref();
    println!("{:?} {:?}", kk[0], kk.len());
    println!("{:?}", b);
}
