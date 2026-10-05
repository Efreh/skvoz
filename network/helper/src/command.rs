//! Only fixed executables and internally generated arguments reach this module.
use crate::{HelperError, Result};
use std::{
    io::{Read, Write},
    os::fd::BorrowedFd,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

pub(crate) enum Tool {
    Ip,
    Nft,
    Conntrack,
    Resolvectl,
    Pkcheck,
}
impl Tool {
    fn path(&self) -> &'static str {
        match self {
            Self::Ip => "/usr/sbin/ip",
            Self::Nft => "/usr/sbin/nft",
            Self::Conntrack => "/usr/sbin/conntrack",
            Self::Resolvectl => "/usr/bin/resolvectl",
            Self::Pkcheck => "/usr/bin/pkcheck",
        }
    }
}
pub(crate) struct Output {
    pub success: bool,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}
pub(crate) fn run(
    tool: Tool,
    args: &[String],
    input: &[u8],
    owner: Option<BorrowedFd<'_>>,
    deadline: Instant,
) -> Result<Output> {
    if input.len() > 512 * 1024
        || args.len() > 64
        || args.iter().any(|a| a.len() > 4096 || a.contains('\0'))
    {
        return Err(HelperError::InvalidRequest);
    }
    let retain_stdout = !matches!(tool, Tool::Conntrack);
    let mut child = Command::new(tool.path())
        .args(args)
        .env_clear()
        .env("PATH", "/usr/sbin:/usr/bin:/sbin:/bin")
        .env("LC_ALL", "C")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let result = (|| {
        use std::os::fd::AsFd;
        let stdin = child.stdin.take().ok_or(HelperError::InvalidState)?;
        let mut stdout = child.stdout.take().ok_or(HelperError::InvalidState)?;
        let mut stderr = child.stderr.take().ok_or(HelperError::InvalidState)?;
        skvoz_network_native::set_nonblocking(stdin.as_fd())?;
        skvoz_network_native::set_nonblocking(stdout.as_fd())?;
        skvoz_network_native::set_nonblocking(stderr.as_fd())?;
        let mut stdin = Some(stdin);
        let mut position = 0;
        let mut out = Vec::new();
        let mut err = Vec::new();
        let deadline = deadline.min(Instant::now() + Duration::from_secs(4));
        loop {
            if Instant::now() >= deadline
                || owner.is_some_and(|fd| skvoz_network_native::owner_closed(fd).unwrap_or(true))
            {
                return Err(HelperError::InvalidState);
            }
            if let Some(pipe) = stdin.as_mut() {
                if position < input.len() {
                    match pipe.write(&input[position..]) {
                        Ok(0) => return Err(HelperError::InvalidState),
                        Ok(n) => position += n,
                        Err(e)
                            if matches!(
                                e.kind(),
                                std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                            ) => {}
                        Err(e) => return Err(e.into()),
                    }
                }
                if position == input.len() {
                    stdin = None;
                }
            }
            drain(
                &mut stdout,
                &mut out,
                if retain_stdout { 512 * 1024 } else { 0 },
            )?;
            drain(&mut stderr, &mut err, 65536)?;
            if let Some(status) = child.try_wait()? {
                drain(
                    &mut stdout,
                    &mut out,
                    if retain_stdout { 512 * 1024 } else { 0 },
                )?;
                drain(&mut stderr, &mut err, 65536)?;
                return Ok(Output {
                    success: status.success(),
                    stdout: out,
                    stderr: err,
                });
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    })();
    if result.is_err() {
        let _ = child.kill();
        // A killed process can remain in uninterruptible kernel sleep. Never
        // turn a finite command deadline into an unbounded wait; fatal helper
        // shutdown leaves final child reaping to its process supervisor.
        let reap_deadline = Instant::now() + Duration::from_millis(100);
        while Instant::now() < reap_deadline {
            if child.try_wait().ok().flatten().is_some() {
                break;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    result
}
fn drain(pipe: &mut impl Read, out: &mut Vec<u8>, cap: usize) -> Result<()> {
    let mut bytes = [0u8; 4096];
    let mut drained = 0;
    loop {
        match pipe.read(&mut bytes) {
            Ok(0) => return Ok(()),
            Ok(n) => {
                drained += n;
                if cap == 0 {
                    if drained >= 65536 {
                        return Ok(());
                    }
                    continue;
                }
                if out.len() + n > cap {
                    return Err(HelperError::Overloaded);
                }
                out.extend_from_slice(&bytes[..n]);
                if drained >= 65536 {
                    return Ok(());
                }
            }
            Err(e)
                if matches!(
                    e.kind(),
                    std::io::ErrorKind::WouldBlock | std::io::ErrorKind::Interrupted
                ) =>
            {
                return Ok(());
            }
            Err(e) => return Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn discarded_large_output_still_yields_to_owner_and_deadline_checks() {
        let mut pipe = std::io::Cursor::new(vec![42; 70000]);
        let mut output = Vec::new();
        drain(&mut pipe, &mut output, 0).unwrap();
        assert_eq!(pipe.position(), 65536);
        assert!(output.is_empty());
        drain(&mut pipe, &mut output, 0).unwrap();
        assert_eq!(pipe.position(), 70000);
    }
}
