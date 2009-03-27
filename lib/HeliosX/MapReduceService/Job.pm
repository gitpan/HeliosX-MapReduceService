package HeliosX::MapReduceService::Job;

use 5.008;
use base qw(Helios::Job);
use strict;
use warnings;

use Error qw(:try);

use Helios::Error;
use HeliosX::MapReduceService::MetajobStatus;

our $VERSION = '0.02_0861';

our $DOD_RETRY_LIMIT = 3;
our $DOD_RETRY_INTERVAL = 5;

=head1 NAME

HeliosX::MapReduceService::Job - Class to represent simple jobs to HeliosX::MapReduceService

=head1 SYNOPSIS



=head1 DESCRIPTION

This module is not yet complete, thus the documentation is not yet complete.

=head1 METHODS

=head1 JOB SUCCESS/FAILURE METHODS

=head2 completed()

=head2 failed([$error,] [$exitstatus])

=head2 failedNoRetry([$error,] [$exitstatus])

The completed(), failed(), and failedNoRetry() methods for 
HeliosX::MapReduceService::Job update the table associated with the job's 
parent metajob.  They will then call the superclass's version of themselves, 
return that version's return value to the calling routine. 

=cut

sub completed {
	my ($self) = @_;
	my $job = $self->job;
	my $args = $self->getArgs();

	if ( defined($args->{parentid}) ) {
        $self->updateMetajobStatus();
	}
	return $self->SUPER::completed(@_);
}


sub failed {
    my ($self, $error, $exitstatus) = @_;
    my $job = $self->job;
    my $args = $self->getArgs();

    if ( defined($args->{parentid}) ) {
        $self->updateMetajobStatus($exitstatus);
    }
    return $self->SUPER::completed(@_);
}


sub failedNoRetry {
    my ($self, $error, $exitstatus) = @_;
    my $job = $self->job;
    my $args = $self->getArgs();

    if ( defined($args->{parentid}) ) {
        $self->updateMetajobStatus($exitstatus);
    }
    return $self->SUPER::completed(@_);
}


=head1 OTHER METHODS

=head2 updateMetajobStatus($status)

=cut

sub updateMetajobStatus {
	my $self = shift;
    my $status = shift;
	my $job = $self->job();
	my $args = $self->getArgs();

    my $retryCount = 0;
    RETRY:
    try {
        my $driver = $self->getDriver();
        HeliosX::MapReduceService::MetajobStatus->install_properties({
            datasource  => 'helios_metajob_status_'.$args->{parentid},
            columns     => [qw(jobid complete_time exitstatus)],
            primary_key => 'jobid'
        });
    
        my $ms = HeliosX::MapReduceService::MetajobStatus->new(
            jobid         => $self->getJobid(),
            complete_time => time(),
            exitstatus    => $status
        );
        $ms->save($ms);
    } otherwise {
        my $e = shift;
        if ($retryCount >= $DOD_RETRY_LIMIT) {
            throw Helios::Error::DatabaseError($e->text);
        } else {
            $retryCount++;
            sleep $DOD_RETRY_INTERVAL;
            next RETRY;
        }
    };

    return 1;
}

1;
__END__


=head1 SEE ALSO

L<HeliosX::MapReduceService>

=head1 AUTHOR

Andrew Johnson, E<lt>lajandy at cpan dotorgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Andrew Johnson

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
