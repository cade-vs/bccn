##############################################################################
#
#  Net::BCCN Broadcast Channel Notify protocol
#  (c) Vladi Belperchinov-Shabanski "Cade" 2026
#  http://cade.noxrun.com <cade@noxrun.com>
#
#  GPL
#
##############################################################################
package Net::BCCN;
use strict;
use POSIX ":sys_wait_h";
use IO::Socket::INET;
use IO::Select;
use Data::Dumper;

our $VERSION = '1.1';

$Data::Dumper::Terse = 1;

##############################################################################


# FIXME: sequence in hex?
sub parse_notification_msg_data
{
  my $data = shift;
  return () unless $data =~ /^BCCN(\d+)\[(\d+)(:([a-z_0-9]+)=([a-z_0-9]+))?\]([a-z_0-9\.\?\/]+):(\d+):(\!?[a-z_0-9\.\/]+)\|(.*)/i;

  #        ver  len  cktype  ckval  fr  seq  chan  payload
  return  ( $1,  $2,     $4,    $5, $6,  $7,   $8,     $9 );
}

##############################################################################

# use Sys::Hostname; my $h = hostname();

sub new
{
  my $class = shift;
  $class = ref( $class ) || $class;

  my %opt = @_;

  my $self = {
               NAME    => $opt{ 'NAME'    } || '?',               # local instance name
               ADDR    => $opt{ 'ADDR'    } || '255.255.255.255', # broadcast address
               BIND    => $opt{ 'BIND'    } || '0.0.0.0',         # local bind address
               PORT    => $opt{ 'PORT'    },                      # which UDP port to listen and send on

               DEBUG   => $opt{ 'DEBUG'   }, # debug level, true to enable or positive number for debug level
             };

  $self->{ 'DD'   } = {} if $opt{ 'DD' }; # dedup requested
  $self->{ 'SSS'  } = {} if $opt{ 'SS' }; # speed stats requested
  $self->{ 'SSX'  } =       $opt{ 'SS' }; # max events counting

  $self->{ 'TQMC' } = {}; # total queue messages count

# print Dumper( \%opt, $self );

  bless $self, $class;
  return $self;
}

sub error
{
  my $self = shift;

  return $self->{ 'ERR' };
}

sub open
{
  my $self = shift;

  my $addr = $self->{ 'ADDR' };
  my $bind = $self->{ 'BIND' };
  my $port = $self->{ 'PORT' };

  $self->{ 'ERR' } = undef;

  my $ss   = IO::Socket::INET->new
    (
    Proto     => "udp",
    PeerAddr  => $addr,
    PeerPort  => $port,
    Broadcast => 1,
    );

  if( ! $ss )
    {
    $self->{ 'ERR' } = "udp send socket to $addr:$port failed: $!";
    return undef;
    }

  if( ! setsockopt( $ss, SOL_SOCKET, SO_BROADCAST, 1 ) )
    {
    $self->{ 'ERR' } = "udp send socket set SO_BROADCAST failed: $!";
    return undef;
    }


  my $rs   = IO::Socket::INET->new
    (
    Proto     => "udp",
    LocalAddr => $bind,
    LocalPort => $port,
    ReuseAddr => 1,
    Blocking  => 0,
    );

  if( ! $rs )
    {
    $self->{ 'ERR' } = "udp recv socket bind $bind:$port failed: $!";
    return undef;
    }

  $self->{ 'SS' } = $ss;
  $self->{ 'RS' } = $rs;

  return 1;
}

sub notify
{
  my $self = shift;

  my $chan = shift;
  my $body = shift;

  my $ss = $self->{ 'SS' } or die "error: cannot notify, send socket not open, call open() first\n";
  $self->{ 'ERR' } = undef;

  my $seq = ++$self->{ 'SQ' }; # send sequence number

  my $name = $self->{ 'NAME' };

  my $msg = "$name:$seq:$chan|$body";
  my $len = length $msg;
  $msg = "BCCN1[$len]$msg";
  $len = length $msg;

  my $sl = $ss->send( $msg );
  if( ! defined( $sl ) )
    {
    dr_log("ERR: send failed: $!");
    $self->{ 'ERR' } = "send to channel [$chan] failed: $!";
    return undef;
    }

  if( $sl != $len )
    {
    $self->{ 'ERR' } = "send to channel [$chan] error: short send: expected $len, sent $sl bytes";
    return undef;
    }

  return 1;
}


sub __pull_all_available
{
  my $self = shift;
  my $chan = shift;
  my $opt  = shift;

  $self->{ 'ERR' } = undef;

  my $rs = $self->{ 'RS' } or die "error: cannot listen, recv socket not open, call open() first\n";
  my $cq = $self->{ 'Q' }{ $chan } ||= []; # channel queue

  my $to = @$cq ? 0 : $opt->{ 'TIMEOUT' } || 0; # if q has messages, do not wait, just pull whatever waiting

  my $sel = IO::Select->new;
  $sel->add( $rs );

  my $recvc = 0;
  while (1)
    {
    my $msg;

    my @ready = $sel->can_read( $to );
    $to = 0;

    last unless @ready;
=pod
    if( ! @ready )
      {
      # no messages
      my $xcq = $self->{ 'Q' }{ $chan } ||= []; # expected channel queue
      last if @$xcq > 0; # exit if no more messages and expected channel q is not empty
      last if $wait > 0; # exit with no message if we did wait some time
      $wait = $to; # no more messages but expected q is empty, wait for more...
      next;
      }
    $wait = 0;
=cut

    my $from = $rs->recv( $msg, 65535, 0 );

    if( ! defined( $from ) )
      {
      next if $!{ 'EINTR' };
      $self->{ 'ERR' } = "recv failed: $!";
      return undef;
      }

    my ( $from_port, $from_ip4_packed ) = unpack_sockaddr_in( $from );
    my $from_ip4 = inet_ntoa( $from_ip4_packed );
    my $len    = length( $msg );

    $recvc++;
    print("recv: #$recvc from $from_ip4:$from_port len=$len [$msg]\n");

    my @msg = parse_notification_msg_data( $msg );
    #  @msg = ver0  len1  cktype2  ckval3  fr4  seq5  chan6  msg7

    next unless @msg;

    if( $self->{ 'DD' } )
      {
      my $key = "$from_ip4:$msg[4]:$msg[5]:$msg[6]";
      next if exists $self->{ 'DD' }{ $key };
      $self->{ 'DD' }{ $key } = time(); # used by clear_dd_lookup()
      }

    my $cq = $self->{ 'Q' }{ $msg[6] } ||= []; # channel queue

    push @$cq, {
               FROM      => $msg[4],
               FROM_IP4  => $from_ip4,
               FROM_PORT => $from_port,
               CHANNEL   => $msg[6],
               MSG       => $msg[7],
               RTIME     => time(),      # receive time
               };

    $self->{ 'TQMC' }{ '*'     }++;
    $self->{ 'TQMC' }{ $msg[6] }++;

    if( $self->{ 'SSS' } )
      {
      $self->{ 'SSS' }{ '*'     } ||= [];
      $self->{ 'SSS' }{ $msg[6] } ||= [];
      push  @{ $self->{ 'SSS' }{ '*'     } }, time();
      push  @{ $self->{ 'SSS' }{ $msg[6] } }, time();
      shift @{ $self->{ 'SSS' }{ '*'     } } while @{ $self->{ 'SSS' }{ '*'     } } > $self->{ 'SSX' };
      shift @{ $self->{ 'SSS' }{ $msg[6] } } while @{ $self->{ 'SSS' }{ $msg[6] } } > $self->{ 'SSX' };
      }
    }

  return $recvc;
}


sub listen
{
  my $self = shift;

  my $chan = shift;
  my $opt  = shift;

  my $cq = $self->{ 'Q' }{ $chan } ||= []; # channel queue

  my $recvc = $self->__pull_all_available( $chan, $opt );

  my $qc = @$cq;
  print "pulled messages in one pass: $recvc, test qc $qc\n";

  return undef unless @$cq > 0;

  return shift @$cq;
}

sub clear_dd_lookup
{
  my $self = shift;
  my $to   = shift; # timeout cleanup, remove all older than $to seconds

  return unless exists $self->{ 'DD' };

  for my $key ( keys %{ $self->{ 'DD' } } )
    {
    delete $self->{ 'DD' }{ $key } if $self->{ 'DD' }{ $key } < time() - $to;
    }
}

sub stats
{
  my $self = shift;

  my %st;

  # queues stats, seen, currently active (has messages), counts...
  my $qsc = 0;
  while( my ( $k, $v ) = each %{ $self->{ 'Q' } } )
    {
    $qsc++;
    push @{ $st{ 'SEEN_QS' } }, $k;
    my $mc = @{ $v };
    push @{ $st{ 'ACTIVE_QS' } }, $k if $mc > 0;
    $st{ 'QMC' }{ $k } = $mc; # queue message count
    }
  $st{ 'QSC' } = $qsc; # queues count

  # dedup stats
  if( exists $self->{ 'DD' } )
    {
    my $ddc = 0;
    my $ddo = time();
    while( my ( $k, $v ) = each %{ $self->{ 'DD' } } )
      {
      $ddc++;
      $ddo = $v if $v < $ddo;
      }
    $st{ 'DDC' } = $ddc; # dedup lookup count
    $st{ 'DDO' } = $ddo; # dedup oldest lookup
    }

  # speed stats for last SS events
  if( exists $self->{ 'SSS' } )
    {
    while( my ( $k, $v ) = each %{ $self->{ 'SSS' } } )
      {
      $st{ 'SS' }{ $k } = @$v / ( $v->[-1] - $v->[0] ) if @$v > 1 and $v->[-1] - $v->[0] > 0;
      }
    }

  # total queue messages count
  while( my ( $k, $v ) = each %{ $self->{ 'TQMC' } } )
    {
    $st{ 'TQMC' }{ $k } = $v;
    }

  return \%st;
}
