//! Terminal peer coordination, independent of DATA credit and its control slot.
use super::{
    closing::{Closing, Prefix},
    *,
};

impl NatsRuntime {
    pub(super) fn start_close(&mut self, id: PeerId) {
        self.retire(id);
        if let Some(s) = self.peers.get_mut(&id).and_then(|p| p.session.as_mut())
            && s.proven
            && s.closing.is_none()
        {
            s.closing = Some(Closing::initiate(
                self.epoch,
                Prefix {
                    sequence: s.tx,
                    amount: s.sender_credit.sent,
                },
                s.retired_at.unwrap() + self.config.terminal_drain_timeout,
            ));
        }
    }

    pub(super) fn receive_close(&mut self, id: PeerId, c: Control) -> Result<(), RuntimeError> {
        if c.serial != 0
            || c.hello == 0
            || c.body_len != 0
            || c.epoch != 0
            || c.applied_records != 0
            || (c.kind == 14 && (c.watermark != 0 || c.records != 0 || c.applied_bytes != 0))
        {
            return Err(RuntimeError::Protocol);
        }
        let s = self
            .peers
            .get(&id)
            .and_then(|p| p.session.as_ref())
            .ok_or(RuntimeError::PeerUnavailable)?;
        if c.kind == 12 {
            let remote = Prefix::from_control(&c)?;
            if !s.recipient_credit.allowed.contains(remote.amount) {
                return Err(RuntimeError::Protocol);
            }
            remote.reached(s.rx, s.recipient_credit.applied)?;
            if let Some(state) = s.closing {
                if state.initiating() && self.config.id < id {
                    return Ok(());
                }
                if !state.initiating() {
                    return if state.nonce == c.hello && state.remote() == Some(remote) {
                        Ok(())
                    } else {
                        Err(RuntimeError::Protocol)
                    };
                }
            } else if s.retiring {
                return Err(RuntimeError::Protocol);
            }
            self.retire(id);
            let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
            s.closing = Some(Closing::respond(
                c.hello,
                Prefix {
                    sequence: s.tx,
                    amount: s.sender_credit.sent,
                },
                remote,
                s.retired_at.unwrap() + self.config.terminal_drain_timeout,
            ));
            return Ok(());
        }
        let s = self.peers.get_mut(&id).unwrap().session.as_mut().unwrap();
        let state = s.closing.as_mut().ok_or(RuntimeError::Protocol)?;
        let receipt = match c.kind {
            13 => {
                let remote = Prefix::from_control(&c)?;
                if !s.recipient_credit.allowed.contains(remote.amount) {
                    return Err(RuntimeError::Protocol);
                }
                remote.reached(s.rx, s.recipient_credit.applied)?;
                state.acknowledged(c.hello, remote)?
            }
            14 => state.done(c.hello)?,
            _ => return Err(RuntimeError::Protocol),
        };
        s.sender_credit.apply_receipt(receipt)?;
        Ok(())
    }

    pub(super) async fn close_work(&mut self, id: PeerId) -> Result<usize, RuntimeError> {
        let drained = self.manager.peer_streams(id).is_none_or(|n| n == 0);
        let Some(s) = self.peers.get_mut(&id).and_then(|p| p.session.as_mut()) else {
            return Ok(0);
        };
        let Some(state) = s.closing.as_mut() else {
            return Ok(0);
        };
        if Instant::now() >= state.deadline && !state.complete() {
            // Even a zero-byte terminal failure cannot be reported as a
            // successful administrative cleanup. A fresh pair proof can clear it.
            self.retired_send.entry(id).or_default();
            s.closing = None;
            self.last_error = Some(RuntimeError::TerminalDrainTimeout);
            return Ok(0);
        }
        let Some(kind) = state.output(s.rx, s.recipient_credit.applied, drained)? else {
            return Ok(0);
        };
        let mut c = Control {
            kind,
            sender: self.epoch,
            recipient: s.generation,
            token: s.token,
            hello: state.nonce,
            ..Control::default()
        };
        if kind != 14 {
            c.watermark = state.local.sequence;
            c.records = state.local.amount.records;
            c.applied_bytes = state.local.amount.bytes;
        }
        self.send_control(id, c, true).await?;
        Ok(1)
    }
}
