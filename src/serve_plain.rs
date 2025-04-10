//! This module is responsible for receiving connections over plaintext TCP
use crate::serve::*;

pub struct PlaintextServer {}

impl Server for PlaintextServer {}
