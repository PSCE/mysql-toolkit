package pt_online_schema_change_plugin;

sub new {
   my $proto = shift;  
   my $class = ref($proto) || $proto;  
   my $self  = {};  

   bless ($self, $class);  

   return $self;  
}

sub before_create_insert_trigger {
   my $self = shift;
   my ( %args ) = @_;
   my @required_args = qw(orig_tbl new_tbl columns Quoter qcols new_vals);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($orig_tbl, $new_tbl, $cols, $q, $qcols, $new_vals) = @args{@required_args};

   $$new_vals = join(', ',
      map {
         exists $orig_tbl->{tbl_struct}->{is_text}->{$_->{old}} ?
            "CONVERT(CONVERT(CONVERT(NEW.".$q->quote($_->{old})." USING latin1) USING binary) USING utf8mb4)" : "NEW.".$q->quote($_->{old})
      } @$cols
   );
}

sub dml_select {
   my $self = shift;
   my ( %args ) = @_;
   my @required_args = qw(orig_tbl new_tbl columns Quoter dml select);
   foreach my $arg ( @required_args ) {
      die "I need a $arg argument" unless $args{$arg};
   }
   my ($orig_tbl, $new_tbl, $cols, $q, $dml, $select) = @args{@required_args};

   $$select = join(', ',
      map {
         exists $orig_tbl->{tbl_struct}->{is_text}->{$_->{old}} ?
            "CONVERT(CONVERT(CONVERT(".$q->quote($_->{old})." USING latin1) USING binary) USING utf8mb4)" : $q->quote($_->{old})
      } @$cols
   );
}

1;
