// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
// Copyright 2009 The gelf_logger Authors. All rights reserved.

use std::sync::mpsc::{Receiver, SyncSender};
use std::thread;
use std::time::Duration;

use serde_gelf::GelfRecord;

use crate::output::GelfTcpOutput;
use crate::result::Error;

/// Enum used to send commands over the channel.
#[derive(Clone, Debug)]
pub enum Event {
    /// Command to force the flush of the buffer.
    Send,
    /// Command used to send a record into the buffer.
    Data(GelfRecord),
}

pub struct Metronome;

impl Metronome {
    pub fn start(frequency: u64, chan: SyncSender<Event>) {
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(frequency));
            let _ = chan.send(Event::Send);
        });
    }
}

pub struct Buffer {
    items: Vec<GelfRecord>,
    arx: Receiver<Event>,
    errors: Vec<Error>,
    output: GelfTcpOutput,
}

impl Buffer {
    pub fn new(arx: Receiver<Event>, output: GelfTcpOutput) -> Buffer {
        Buffer { items: Vec::new(), arx, errors: Vec::new(), output }
    }
    pub fn run(&mut self) {
        loop {
            match { self.arx.recv() } {
                Ok(event) => {
                    match event {
                        Event::Send => match self.output.send(&self.items) {
                            Ok(_) => self.items.clear(),
                            Err(exc) => {
                                self.errors.push(exc);
                                if self.errors.len() >= 5 {
                                    eprintln!("Many errors occurred while sending GELF logs event!");
                                    for err in self.errors.iter() {
                                        eprintln!(">> {:?}", err);
                                    }
                                    self.errors.clear();
                                }
                                thread::sleep(Duration::from_millis(100));
                            }
                        },
                        Event::Data(record) => self.items.push(record),
                    }
                }
                Err(_) => return,
            };
        }
    }
}
