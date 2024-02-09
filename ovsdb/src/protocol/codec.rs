use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::{Decoder, Encoder},
};

use super::Message;

pub enum BufferTag {
    Obj,
    Str,
}

#[derive(Default)]
pub struct Codec {
    data: Vec<u8>,
    tags: Vec<BufferTag>,
}

impl Codec {
    pub fn new() -> Self {
        Self::default()
    }

    fn try_decode_message(
        &mut self,
        src: &[u8],
    ) -> Result<(Option<Message>, usize), serde_json::Error> {
        let mut offset = 0;

        while offset < src.len() {
            match self.tags.last() {
                Some(BufferTag::Str) => {
                    if let Some(n) = &src[offset..].iter().position(|&c| c == b'"') {
                        offset += n + 1;
                        self.tags.pop();
                        continue;
                    } else {
                        break;
                    }
                }
                Some(BufferTag::Obj) => {
                    if let Some(n) = &src[offset..]
                        .iter()
                        .position(|&c| [b'"', b'{', b'}'].contains(&c))
                    {
                        offset += n;
                        let char = src[offset];
                        offset += 1;
                        match &char {
                            b'"' => self.tags.push(BufferTag::Str),
                            b'{' => self.tags.push(BufferTag::Obj),
                            b'}' => {
                                self.tags.pop();
                                if self.tags.is_empty() {
                                    // We have a full object
                                    self.data.extend_from_slice(src);
                                    let msg: Message = serde_json::from_slice(&self.data.to_vec())?;
                                    self.data.clear();
                                    return Ok((Some(msg), offset));
                                }
                            }
                            _ => unreachable!(),
                        }
                    } else {
                        break;
                    }
                }
                None => {
                    if let Some(n) = &src[offset..].iter().position(|&c| c == b'{') {
                        offset += n + 1;
                        self.tags.push(BufferTag::Obj);
                    } else {
                        panic!("Datastream corrupted.  No openening tag.");
                    }
                }
            }
        }

        self.data.extend_from_slice(src);
        Ok((None, src.len()))
    }
}

impl Decoder for Codec {
    type Item = Message;
    type Error = crate::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        let (res, consume) = self.try_decode_message(src.chunk())?;
        src.advance(consume);
        Ok(res)
    }
}

impl Encoder<Message> for Codec {
    type Error = crate::Error;

    fn encode(&mut self, item: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let data = serde_json::to_vec(&item)?;
        dst.reserve(data.len());
        dst.put_slice(&data);
        Ok(())
    }
}