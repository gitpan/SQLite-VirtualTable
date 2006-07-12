package SQLite::VirtualTable::Util;

require Exporter;

our @ISA = qw(Exporter);
our @EXPORT_OK = qw(unescape);

my %esc = ( "\n" => 'n',
	    "\r" => 'r',
	    "\t" => 't' );
my %unesc = reverse %esc;

sub unescape {
    my $s = shift;
    $s =~ s{\\([tnr\\"' =:#!])|\\u([\da-fA-F]{4})|["']}{
                defined $1 ? $unesc{$1}||$1 :
                defined $2 ? chr hex $2 :
                '';
           }ge;
    $s;
}

1;
