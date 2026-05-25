/* bccn.c - implementation of the minimal BCCN1 baseline. */

#include "bccn.h"

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <inttypes.h>

/* -- helpers -- */

static int contains_forbidden( const char *s, const char *bad )
  {
  while (*s)
    {
    if (strchr( bad, *s ))
      {
      return 1;
      }
    s++;
    }
  return 0;
  }

static const uint8_t * memchr_byte( const uint8_t *buf, size_t n, uint8_t b )
  {
  for (size_t i = 0; i < n; i++)
    {
    if (buf[ i ] == b)
      {
      return &buf[ i ];
      }
    }
  return NULL;
  }

/* -- API -- */

int bccn_init( bccn_conn_t *c, int fd, const struct sockaddr_in *dst )
  {
  if (!c || fd < 0 || !dst)
    {
    printf( "bccn_init: bad arg" );
    return -1;
    }
  memset( c, 0, sizeof *c );
  c->fd = fd;
  c->dst = *dst;
  strcpy( c->src, "?" );             /* unknown-sender default */
  c->seq = 1;
  return 0;
  }

void bccn_close( bccn_conn_t *c )
  {
  if (!c)
    {
    return;
    }
  /* Does not close fd: caller owns the socket. */
  memset( c, 0, sizeof *c );
  c->fd = -1;
  }

int bccn_set_name( bccn_conn_t *c, const char *name )
  {
  if (!c)
    {
    return -1;
    }
  if (!name || !*name)
    {
    strcpy( c->src, "?" );
    return 0;
    }
  if (strlen( name ) > BCCN_MAX_SRC)
    {
    printf( "bccn_set_name: name too long (%zu > %d)",
            strlen( name ), BCCN_MAX_SRC );
    return -1;
    }
  if (contains_forbidden( name, " \t\r\n|:" ))
    {
    printf( "bccn_set_name: name contains forbidden character" );
    return -1;
    }
  strcpy( c->src, name );
  return 0;
  }

const char * bccn_get_name( const bccn_conn_t *c )
  {
  if (!c)
    {
    return "?";
    }
  return c->src;
  }

void bccn_set_seq( bccn_conn_t *c, uint64_t n )
  {
  if (c)
    {
    c->seq = n;
    }
  }

uint64_t bccn_get_seq( const bccn_conn_t *c )
  {
  return c ? c->seq : 0;
  }

int bccn_send( bccn_conn_t *c,
               const char *chan,
               const uint8_t *payload,
               size_t payload_len )
  {
  if (!c || c->fd < 0 || !chan)
    {
    printf( "bccn_send: bad arg" );
    return -1;
    }
  if (!payload && payload_len > 0)
    {
    printf( "bccn_send: NULL payload with non-zero length" );
    return -1;
    }
  if (payload_len > BCCN_MAX_PAYLOAD)
    {
    printf( "bccn_send: payload too large (%zu > %d)",
            payload_len, BCCN_MAX_PAYLOAD );
    return -1;
    }
  if (strlen( chan ) > BCCN_MAX_CHAN)
    {
    printf( "bccn_send: chan too long" );
    return -1;
    }
  if (contains_forbidden( chan, " \t\r\n|:" ))
    {
    printf( "bccn_send: chan contains forbidden character" );
    return -1;
    }
  if (contains_forbidden( c->src, " \t\r\n|:" ))
    {
    printf( "bccn_send: src contains forbidden character" );
    return -1;
    }

  /* Build body: "<src>:<seq>:<chan>|<payload>" */
  uint8_t buf[ BCCN_MAX_DATAGRAM ];
  int n = snprintf( (char *) buf, sizeof buf,
                    "BCCN1[%%zu]%s:%" PRIu64 ":%s|",
                    c->src, c->seq, chan );
  /* Two-pass build: snprintf into a temp body buffer first so we know
     its exact length, then write the envelope + body into buf. */
  uint8_t body[ BCCN_MAX_DATAGRAM ];
  int blen = snprintf( (char *) body, sizeof body,
                       "%s:%" PRIu64 ":%s|",
                       c->src, c->seq, chan );
  if (blen < 0 || (size_t) blen + payload_len > sizeof body)
    {
    printf( "bccn_send: body too large" );
    return -1;
    }
  if (payload_len > 0)
    {
    memcpy( body + blen, payload, payload_len );
    }
  size_t body_total = (size_t) blen + payload_len;

  /* Envelope: "BCCN1[<len>]" + body */
  n = snprintf( (char *) buf, sizeof buf,
                "BCCN1[%zu]", body_total );
  if (n < 0 || (size_t) n + body_total > sizeof buf)
    {
    printf( "bccn_send: envelope too large" );
    return -1;
    }
  memcpy( buf + n, body, body_total );
  size_t total = (size_t) n + body_total;

  ssize_t sent = sendto( c->fd, buf, total, 0,
                         (struct sockaddr *) &c->dst, sizeof c->dst );
  if (sent < 0)
    {
    printf( "bccn_send: sendto: %s", strerror( errno ) );
    return -1;
    }
  if ((size_t) sent != total)
    {
    printf( "bccn_send: short send: %zd/%zu", sent, total );
    return -1;
    }
  c->seq++;
  return 0;
  }

int bccn_recv( bccn_conn_t *c, bccn_msg_t *out )
  {
  if (!c || c->fd < 0 || !out)
    {
    printf( "bccn_recv: bad arg" );
    return -1;
    }

  socklen_t peer_len = sizeof out->peer;
  ssize_t n = recvfrom( c->fd, c->rxbuf, sizeof c->rxbuf, 0,
                        (struct sockaddr *) &out->peer, &peer_len );
  if (n < 0)
    {
    printf( "bccn_recv: recvfrom: %s", strerror( errno ) );
    return -1;
    }
  if (n == 0)
    {
    printf( "bccn_recv: zero-length datagram" );
    return -1;
    }

  /* Envelope: BCCN1[<len>]<body> */
  static const char magic[] = "BCCN1[";
  size_t mlen = sizeof magic - 1;
  if ((size_t) n < mlen + 2 || memcmp( c->rxbuf, magic, mlen ) != 0)
    {
    printf( "bccn_recv: bad magic" );
    return -1;
    }

  const uint8_t *rb = memchr_byte( c->rxbuf + mlen, n - mlen, ']' );
  if (!rb)
    {
    printf( "bccn_recv: envelope: no ']'" );
    return -1;
    }

  /* Length string lives between c->rxbuf+mlen and rb. */
  size_t meta_len = (size_t)(rb - (c->rxbuf + mlen));
  if (meta_len == 0 || meta_len > 31)
    {
    printf( "bccn_recv: envelope: bad meta length" );
    return -1;
    }
  char meta[ 32 ];
  memcpy( meta, c->rxbuf + mlen, meta_len );
  meta[ meta_len ] = '\0';

  char *endp = NULL;
  unsigned long body_len = strtoul( meta, &endp, 10 );
  if (!endp || *endp != '\0')
    {
    printf( "bccn_recv: envelope: bad length %s", meta );
    return -1;
    }

  const uint8_t *body = rb + 1;
  size_t body_actual = (size_t) n - (size_t)(body - c->rxbuf);
  if (body_actual != body_len)
    {
    printf( "bccn_recv: length mismatch: header=%lu actual=%zu",
            body_len, body_actual );
    return -1;
    }

  /* Body: <src>:<seq>:<chan>|<payload> */
  const uint8_t *pipe = memchr_byte( body, body_actual, '|' );
  if (!pipe)
    {
    printf( "bccn_recv: body: no '|'" );
    return -1;
    }
  size_t hdr_len = (size_t)(pipe - body);

  /* Split header on first two ':'. */
  const uint8_t *c1 = memchr_byte( body, hdr_len, ':' );
  if (!c1)
    {
    printf( "bccn_recv: body: header missing first ':'" );
    return -1;
    }
  const uint8_t *c2 = memchr_byte( c1 + 1,
                                   hdr_len - (size_t)(c1 + 1 - body),
                                   ':' );
  if (!c2)
    {
    printf( "bccn_recv: body: header missing second ':'" );
    return -1;
    }

  size_t src_len  = (size_t)(c1 - body);
  size_t seq_len  = (size_t)(c2 - c1 - 1);
  size_t chan_len = (size_t)(pipe - c2 - 1);

  if (src_len > BCCN_MAX_SRC || chan_len > BCCN_MAX_CHAN || seq_len == 0
      || seq_len > 20)
    {
    printf( "bccn_recv: body: field lengths out of bounds" );
    return -1;
    }

  memcpy( out->src, body, src_len );
  out->src[ src_len ] = '\0';

  char seq_str[ 32 ];
  memcpy( seq_str, c1 + 1, seq_len );
  seq_str[ seq_len ] = '\0';
  endp = NULL;
  unsigned long long seq = strtoull( seq_str, &endp, 10 );
  if (!endp || *endp != '\0')
    {
    printf( "bccn_recv: body: bad seq %s", seq_str );
    return -1;
    }
  out->seq = (uint64_t) seq;

  memcpy( out->chan, c2 + 1, chan_len );
  out->chan[ chan_len ] = '\0';

  out->payload     = pipe + 1;
  out->payload_len = body_actual - (size_t)(pipe + 1 - body);

  return 0;
  }
