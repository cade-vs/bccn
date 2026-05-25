# NAME

Net::BCCN - Broadcast Channel Notifications protocol

# SYNOPSIS

    use Net::BCCN;

    my $bccn = Net::BCCN->new(
      NAME  => "host1/worker/$$",
      ADDR  => '255.255.255.255',
      BIND  => '0.0.0.0',
      PORT  => 1122,
      DD    => 1,         # enable per-message dedup
      SS    => 100,       # track speed stats over last 100 messages
      DEBUG => 0,
    );

    $bccn->open() or die "open failed: " . $bccn->error();

    # publish
    $bccn->notify( 'supply-channel',
                   'txnid=12345:avail=1234:rc=00' );

    # listen on a channel (returns one message, or undef if none waiting)
    while (1)
      {
      my $msg = $bccn->listen( 'work-channel', { TIMEOUT => 1 } );
      next unless $msg;
      process( $msg );
      }

# DESCRIPTION

Net::BCCN implements the BCCN wire protocol - a lightweight
LISTEN/NOTIFY-style notification fabric over UDP broadcast on a
single network segment. Inspired by PostgreSQL's NOTIFY/LISTEN,
but extended across machines and across processes that don't
share a database connection.

Each datagram has the form:

    BCCN1[<len><:<algo>=<sum>>?]<src>:<seq>:<chan>|<payload>

    <len>      decimal byte count of the body after "]"
    <algo>     optional integrity algorithm name (e.g. "hmac")
    <sum>      optional integrity check value
    <src>      sender identifier ("from")
    <seq>      sender's per-process sequence counter
    <chan>     channel name ("to")
    <payload>  opaque application-defined bytes

The current implementation handles plain BCCN1 datagrams (no
integrity check). HMAC and directed-channel addressing are part
of the protocol design but not yet implemented in this module.

Semantics are fire-and-forget. No acks, no persistence, no
replay. Suits notify-fabric use cases where occasional packet
loss is acceptable.

# CONSTRUCTOR

## new( %options )

Creates a new BCCN endpoint. Does not open the socket - call
open() to do that.

Options:

- NAME

    The local instance's name string used in outgoing datagrams as source name.
    Defaults to "?" (the protocol's unknown-sender placeholder).
    A typical convention is `host/program/pid`, e.g.
    `host4/processor/12345`.

- ADDR

    Broadcast destination address. Defaults to `255.255.255.255`
    (limited broadcast - never crosses routers). For directed
    subnet broadcast, use the subnet's broadcast address such as
    `10.0.0.255`.

- BIND

    Local bind address. Defaults to `0.0.0.0` (all interfaces).

- PORT

    UDP port to bind and send on. Required. Sender and listener
    must agree on the port number.

- DD

    If true, enable per-message deduplication. The key is the concatenation of
    (sender IP, src, seq, chan); duplicates within this key set are
    silently dropped. Useful when the application sends each
    notification twice for loss reduction. Periodically call
    clear\_dd\_lookup() to expire old entries.

- SS

    If a positive integer N, enable speed statistics tracking over
    the last N messages. Per-channel and aggregate rates are
    exposed via stats(). If unset, no speed tracking.

- DEBUG

    True value or positive integer to enable debug output.
    Currently used for selective trace prints during recv.

# METHODS

## open()

Creates the UDP socket, sets SO\_REUSEADDR, SO\_BROADCAST, and
non-blocking mode, and binds to BIND:PORT. Returns 1 on
success, undef on failure (call error() for details).

## notify( $chan, $payload )

Builds a BCCN1 datagram with the current NAME as src, the next
sequence number, the given channel and payload, and broadcasts
it. Returns 1 on success, undef on failure (call error()).

The channel name is opaque to the protocol; sender and
receivers agree on its meaning. Any whitespace-free string is valid.

The payload is opaque bytes; format is application's choice.
Keep the whole datagram under 1400 bytes to avoid IP
fragmentation.

## listen( $chan, \\%opt )

Pulls all currently available datagrams off the socket into
per-channel queues, then returns one queued message from the
named channel or undef if none is available.

Note that even the sender (caller of notify()) will receive its own
message if calls listen() on the same channel as notify(). BCCN is
just a transport, execution logic is up to the implementation of the
client.

Options:

- TIMEOUT

    Seconds to wait for the first datagram if the channel queue is
    empty. 0 (default) means non-blocking. Once at least one
    datagram has been queued, the method drains everything else
    that's immediately readable without further waiting.

Returned messages are hashrefs:

    {
      FROM      => <src>,           # sender identifier from datagram
      FROM_IP4  => <ip address>,    # sender IP from recvfrom()
      FROM_PORT => <udp port>,      # sender port from recvfrom()
      CHANNEL   => <chan>,
      MSG       => <payload>,
      RTIME     => <unix time>,     # receive time
    }

Note: datagrams for other channels read off the socket in the
same pass are queued internally; subsequent listen() calls on
those channels will return them.

The idea for the TIMEOUT is to allow shortest possible sleep between
processing and housekeeping (when no message available and timeout expired).
So calling listen() with timeout will check for incoming messages, and if any,
will read them all and return either requested channel message or undef if none.
When no messages are available it will block until timeout expired or until
any new message arrive (regardless the requested channel or not). On returning
undef, housekeeping or other work can be done.

## error()

Returns the last error message, or undef if the last operation
succeeded. Cleared at the start of each operation that sets it.

## stats()

Returns a hashref of runtime statistics:

    {
      QSC       => <count>,             # total queues/channels count
      SEEN_QS   => [ <chan>, ... ],     # all queues/channels ever seen
      ACTIVE_QS => [ <chan>, ... ],     # queues/channels with queued messages
      QMC       => { <chan> => <count>, ... },   # per-channel queue length

      TQMC      => { '*' => <count>,    # total messages received
                     <chan> => <count>, # per-queue/channel total count
                     ... },

      DDC       => <count>,             # dedup table size  (if DD enabled)
      DDO       => <unix time>,         # oldest dedup entry (unix time)

      SS        => { '*' => <rate>,     # messages per second (if SS enabled)
                     <chan> => <rate>,  # per-queue/channel speed
                     ... },
    }

## clear\_dd\_lookup( $seconds )

Removes dedup entries older than $seconds. Call periodically
when DD is enabled to prevent unbounded growth of the dedup
table. Has no effect when DD is disabled. call without argument or 0 to
clear the entire table.

# PROTOCOL DETAILS

The wire format is a printable ASCII envelope plus an opaque
payload. The envelope is fixed across protocol versions; only
the body interpretation changes between BCCN1, a hypothetical
BCCN2, etc. Anything an intermediary needs for routing,
authentication, or framing lives in the envelope; everything
else is body.

This module accepts the optional `:<algo>=<sum>`
integrity slot in the envelope when parsing (the parser regex
allows it) but does not currently verify or generate it.

Reserved characters:

- Space, tab, CR, LF: not allowed in src or chan.
- `|`: separates body header from payload; first occurrence
marks the boundary.
- `:`: separates src/seq/chan in the body header; not allowed
inside src or chan.
- `[ ]`: envelope brackets.
- `!` as first character of chan: reserved for all-form ("!")
and directed-form ("!&lt;target>") delivery. Not implemented in
this module yet.
- `?` as exact value of src: reserved unknown-sender placeholder.

# LIMITATIONS

This module implements a minimal BCCN1 baseline. Not yet
supported:

- HMAC or other integrity check (envelope syntax recognised but
not verified).
- `!`-prefix channel forms: all-broadcast and directed delivery.
- Sequence-number-based per-sender dedup (only IP+src+seq+chan
tuple dedup via the DD option).
- Multicast delivery. The current revision is broadcast-only on
a single L2 segment. Multicast is considered for next releases.

# EXAMPLE

A small publisher/subscriber pair:

    # publisher.pl
    use Net::BCCN;
    my $b = Net::BCCN->new( NAME => "tx-source/$$",
                            PORT => 5400 );
    $b->open() or die $b->error();
    my $i = 0;
    while (1)
      {
      $b->notify( 'demo/tick', "i=$i ts=" . time() );
      $i++;
      sleep 1;
      }

    # subscriber.pl
    use Net::BCCN;
    my $b = Net::BCCN->new( NAME => "tx-sink/$$",
                            PORT => 5400,
                            DD   => 1 );
    $b->open() or die $b->error();
    while (1)
      {
      my $msg = $b->listen( 'demo/tick', { TIMEOUT => 1 } );
      next unless $msg;
      print "$msg->{FROM_IP4} $msg->{FROM} -> $msg->{MSG}\n";
      }

Run multiple subscribers on the same host - all of them will
receive every datagram because the underlying socket has
SO\_REUSEADDR set.

# SEE ALSO

PostgreSQL LISTEN/NOTIFY - the inspiration for BCCN's semantics.

[IO::Socket::INET](https://metacpan.org/pod/IO%3A%3ASocket%3A%3AINET), [IO::Select](https://metacpan.org/pod/IO%3A%3ASelect) - the Perl networking
primitives used internally.

# AUTHOR

    Vladi Belperchinov-Shabanski "Cade" <cade@noxrun.com>
    <http://cade.noxrun.com>
    https://github.com/cade-vs/bccn

# COPYRIGHT AND LICENSE

Copyright (c) 2026 Vladi Belperchinov-Shabanski.

This module is free software; you can redistribute it and/or
modify it under the terms of the GNU General Public License,
version 2 or, at your option, any later version.
