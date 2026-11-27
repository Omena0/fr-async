# `MessageWithSocket`

Base class for socket messages. It extends
[BackgroundMessage](background-message.md) and stores a socket in its public
`socket` attribute.

**Source:** [fr_async/messages.py:141](fr_async/messages.py:141)

## Availability

The class is defined in `fr_async.messages` but is omitted from that module's
`__all__`. It can be imported explicitly, but it is not part of the module's
star-import export list.

## Subclasses

- [WaitReadable](wait-readable.md)
- [WaitWritable](wait-writable.md)
- [Accept](accept.md)

Applications normally use those concrete message types instead of subclassing
`MessageWithSocket` directly.
