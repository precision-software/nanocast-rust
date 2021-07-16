use super::super::*;


pub fn test() {
    let mut stream = BroadcastStream::new();
    let mut read_position: StreamPosition = 0;

    // Verify an empty write returns an empty result.
    assert_eq!(0usize, stream.copy_in(&[0u8;0]));
    assert_eq!(Some((0usize, 0usize)), stream.copy_out(0));

    // Verify we get back the data we wrote.
    let hello_data = "Hello, World".as_bytes();
    assert_eq!(hello_data.len(), stream.copy_in(hello_data));
    let out = stream.copy_out(read_position);
    assert_eq!(Some((0usize, hello_data.len())), out);
    read_position += hello_data.len() as StreamPosition;

    // Add enough data to overfill the buffer. TODO: make each byte different.
    //   We should only fill to the end of the buffer.
    let wrap_data_1 = &[255u8; BUFFER_SIZE+1];
    assert_eq!(BUFFER_SIZE-hello_data.len(), stream.copy_in(wrap_data_1));
    assert_eq!(BUFFER_SIZE_U64, stream.position);

    // We now have wrapped around. Add data to start filling the buffer from the start.
    //   (Must be shorter than hello_data)
    let wrap_data_2 = &[244u8; 10];
    assert_eq!(wrap_data_2.len(), stream.copy_in(wrap_data_2));

    // Verify we overflowed for slow reader.
    assert_eq!(None, stream.copy_out(0));

    // Verify partial read works for up-to-date reader.
    let out1 = stream.copy_out(read_position);
    assert_eq!(Some((hello_data.len() as usize, BUFFER_SIZE)), out1);
    let (start1, end1) = out1.unwrap();
    for b in &stream.buffer[start1..end1] {
        assert_eq!(255u8, *b );
    }
    read_position += (end1-start1) as StreamPosition;

    //  Verify the second partial read gets remainder of data.
    let out2 = stream.copy_out(read_position);
    assert_eq!( Some( (0usize, wrap_data_2.len())), out2);
    let (start2, end2) = out2.unwrap();
    read_position += wrap_data_2.len() as StreamPosition;
    for b in &stream.buffer[start2..end2] {
        assert_eq!(244u8, *b );
    }
}
