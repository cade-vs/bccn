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

print "+++++++++++++++++++ ( $1,  $2,     $4,    $5, $6,  $7,   $8,     $9 )\n";

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


# listen --> rename to __pull_all_available(), deduplicate, push in queues by channel
# called by listen() and then listen() will return first available for the channel from the queue

sub __pull_all_available
{
  my $self = shift;

  $self->{ 'ERR' } = undef;

  my $rs = $self->{ 'RS' } or die "error: cannot listen, recv socket not open, call open() first\n";

  my $sel = IO::Select->new;
  $sel->add( $rs );

  my $recvc = 0;
  while (1)
    {
    my $msg;

    my @ready = $sel->can_read( 4 );

    last unless @ready;

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
    print("recv: #$recvc from $from_ip4:$from_port len=$len [$msg]");

    my @msg = parse_notification_msg_data( $msg );
    #  @msg = ver0  len1  cktype2  ckval3  fr4  seq5  chan6  msg7

    next unless @msg;

    next if $self->{ 'DD' }{ "$from_ip4:$from_port:$msg[4]:$msg[5]:$msg[6]" }++; # TODO: clear dedup q

    my $cq = $self->{ 'Q' }{ $msg[6] } ||= []; # channel queue

    push @$cq, {
               FROM      => $msg[4],
               FROM_IP4  => $from_ip4,
               FROM_PORT => $from_port,
               CHANNEL   => $msg[6],
               MSG       => $msg[7],
               RTIME     => time(),
               };

    print Dumper( 'PULL PULL PULL PULL PULL PULL PULL PULL PULL PULL PULL PULL PULL ', $cq->[-1] );
    }

  return $recvc;
}


sub listen
{
  my $self = shift;

  my $chan = shift;

  my $cq = $self->{ 'Q' }{ $chan } ||= []; # channel queue

  $self->__pull_all_available();

print Dumper( 'QQQQQQQQQQQQQQQQQQQQQQQQQQQQQQQ', $self );

  return undef unless @$cq > 0;

  return shift @$cq;
}
