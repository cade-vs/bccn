#!/usr/bin/perl
use strict;
use lib '../lib';
use Net::BCCN;
use Data::Dumper;

my $nt = new Net::BCCN PORT => 1122;

$nt->open() or die "cannot open sockets: " . $nt->err();

print Dumper( $nt );



my $msg;
$msg = $nt->listen( 'test' );
print Dumper( 'RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR', $msg );

sleep 5;
$msg = $nt->listen( 'test' );
print Dumper( 'RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR', $msg );
$msg = $nt->listen( 'test' );
print Dumper( 'RRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRRR', $msg );
