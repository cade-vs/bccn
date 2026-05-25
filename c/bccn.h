/* bccn.h - minimal BCCN1: send/recv with envelope+body framing.
 *
 * Caller owns the UDP socket. This library only serialises and
 * parses BCCN1 datagrams plus tracks src and seq.
 */

#ifndef BCCN_H
#define BCCN_H

#include <stdint.h>
#include <stddef.h>
#include <sys/socket.h>
#include <netinet/in.h>

#define BCCN_MAX_DATAGRAM   1500
#define BCCN_MAX_SRC        128
#define BCCN_MAX_CHAN      1024
#define BCCN_MAX_PAYLOAD   1300

/* Result of parsing a received datagram. payload points into a buffer
 * owned by the caller; copy it if you need to retain it after the next
 * bccn_recv() call. */
typedef struct
  {
  char            src[ BCCN_MAX_SRC + 1 ];
  uint64_t        seq;
  char            chan[ BCCN_MAX_CHAN + 1 ];
  const uint8_t * payload;
  size_t          payload_len;
  struct sockaddr_in peer;            /* sender address from recvfrom */
  } bccn_msg_t;

typedef struct
  {
  int                 fd;             /* caller-owned UDP socket */
  struct sockaddr_in  dst;            /* destination for outgoing */
  char                src[ BCCN_MAX_SRC + 1 ];
  uint64_t            seq;

  uint8_t             rxbuf[ BCCN_MAX_DATAGRAM ];   /* recv scratch */
  } bccn_conn_t;

/* Initialise a connection over an already-configured UDP socket.
 * The caller is responsible for binding, SO_BROADCAST or multicast
 * group join, SO_REUSEADDR, non-blocking flag, etc.
 *
 * dst is the destination address used by bccn_send().
 * Returns 0 on success, -1 on error. */
int bccn_init( bccn_conn_t *c, int fd, const struct sockaddr_in *dst );

/* Reset the connection (clears src and seq state). Does NOT close fd. */
void bccn_close( bccn_conn_t *c );

/* Set/get the sender's src ("from" identity). NULL or empty string
 * reverts src to "?" (unknown-sender placeholder). */
int  bccn_set_name( bccn_conn_t *c, const char *name );
const char * bccn_get_name( const bccn_conn_t *c );

/* Override or read the outgoing sequence counter. */
void     bccn_set_seq( bccn_conn_t *c, uint64_t n );
uint64_t bccn_get_seq( const bccn_conn_t *c );

/* Send a BCCN1 datagram with the given chan and payload.
 * Returns 0 on success, -1 on error. */
int bccn_send( bccn_conn_t *c,
               const char *chan,
               const uint8_t *payload,
               size_t payload_len );

/* Blocking recv. Fills *out on success. payload pointer in *out
 * points into c->rxbuf; valid until the next bccn_recv() call.
 * Returns 0 on success, -1 on error or malformed datagram. */
int bccn_recv( bccn_conn_t *c, bccn_msg_t *out );

#endif /* BCCN_H */
