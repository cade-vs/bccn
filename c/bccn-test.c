/* example.c - using bccn over a caller-owned UDP socket.
 *
 * The caller decides:
 *   -- bind address and port
 *   -- SO_BROADCAST, multicast, or unicast
 *   -- destination address
 *   -- blocking or non-blocking
 *
 * This example uses broadcast on the local segment.
 */
#include "bccn.h"
#include "printf.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <inttypes.h>

int main( int argc, char **argv )
  {
  int fd = socket( AF_INET, SOCK_DGRAM, 0 );
  if (fd < 0)
    {
    printf( "socket: %s\n", strerror( errno ) );
    return 1;
    }

  int one = 1;
  if (setsockopt( fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof one ) < 0)
    {
    printf( "SO_REUSEADDR: %s\n", strerror( errno ) );
    close( fd );
    return 1;
    }
  if (setsockopt( fd, SOL_SOCKET, SO_BROADCAST, &one, sizeof one ) < 0)
    {
    printf( "SO_BROADCAST: %s\n", strerror( errno ) );
    close( fd );
    return 1;
    }

  struct sockaddr_in laddr;
  memset( &laddr, 0, sizeof laddr );
  laddr.sin_family      = AF_INET;
  laddr.sin_addr.s_addr = htonl( INADDR_ANY );
  laddr.sin_port        = htons( 1122 );

  if (bind( fd, (struct sockaddr *) &laddr, sizeof laddr ) < 0)
    {
    printf( "bind: %s\n", strerror( errno ) );
    close( fd );
    return 1;
    }

  struct sockaddr_in dst;
  memset( &dst, 0, sizeof dst );
  dst.sin_family = AF_INET;
  dst.sin_port   = htons( 1122 );
  inet_pton( AF_INET, "255.255.255.255", &dst.sin_addr );

  bccn_conn_t conn;
  if (bccn_init( &conn, fd, &dst ) < 0)
    {
    close( fd );
    return 1;
    }

  char name[ 64 ];
  snprintf( name, sizeof name, "relay01/cardsys-relay/%d", getpid() );
  bccn_set_name( &conn, name );

  /* Send one message */
  const char *payload = "txnid=12345|amount=1234|rc=00";
  if (bccn_send( &conn,
                 "test",
                 (const uint8_t *) payload,
                 strlen( payload ) ) < 0)
    {
    printf( "send failed\n" );
    }

  /* Receive loop */
  while (1)
    {
    bccn_msg_t msg;
    if (bccn_recv( &conn, &msg ) < 0)
      {
      continue;
      }
    char peer_ip[ INET_ADDRSTRLEN ];
    inet_ntop( AF_INET, &msg.peer.sin_addr, peer_ip, sizeof peer_ip );
    printf( "rx from=%s seq=%" PRIu64 " chan=%s payload_len=%zu peer=%s:%u\n",
            msg.src, msg.seq, msg.chan, msg.payload_len,
            peer_ip, ntohs( msg.peer.sin_port ) );
    /* payload bytes live in conn.rxbuf and are valid until next recv. */
    }

  bccn_close( &conn );
  close( fd );
  return 0;
  }
