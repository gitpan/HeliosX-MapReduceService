package HeliosX::MapReduceService::Metajob;

use 5.008;
use base qw(Helios::Job);
use strict;
use warnings;

use Error qw(:try);

use Helios::Error;
use HeliosX::MapReduceService::BurstError;

our $VERSION = '0.02_0911';

=head1 NAME

HeliosX::MapReduceService::Metajob - Class to represent metajobs to HeliosX::MapReduceService

=head1 SYNOPSIS

  use HeliosX::Metajob;
  blah blah blah

=head1 DESCRIPTION

This module is not yet complete, thus the documentation is not yet complete.

=head1 ACCESSOR METHODS

=cut

sub setJobCount { $_[0]->{jobCount} = $_[1]; }
sub getJobCount { return $_[0]->{jobCount}; }

sub setCompleteJobCount { $_[0]->{completeJobCount} = $_[1]; }
sub getCompleteJobCount { return $_[0]->{completeJobCount}; }

sub setSuccessfulJobCount { $_[0]->{successJobCount} = $_[1]; }
sub getSuccessfulJobCount { return $_[0]->{successJobCount}; }

sub setFailedJobCount { $_[0]->{failedJobCount} = $_[1]; }
sub getFailedJobCount { return $_[0]->{failedJobCount}; }

sub setMetajobIdTable { $_[0]->{metajobIdTable} = $_[1]; }
sub getMetajobIdTable { return $_[0]->{metajobIdTable}; }

sub setMetajobStatusTable { $_[0]->{metajobStatusTable} = $_[1]; }
sub getMetajobStatusTable { return $_[0]->{metajobStatusTable}; }


=head1 METHODS

=head2 parseArgs()

Helios::Metajob's parseArgs() only does metajobs.

#[] parse <jobs> section here?

=cut

sub parseArgs {
	my $self = shift;
	my $job = $self->job();
	my $args;
	my $parsedxml = $self->parseArgXML($job->arg()->[0]);
	# is this a metajob?
	if ( defined($parsedxml->{metajob}) ) {
		# this is a metajob, with full xml syntax (required for metajobs)
		$args = $parsedxml->{metajob};
	} else {
		# uh, the <metajob> tag is _required_
		throw Helios::Error::InvalidArg('<metajob> section not defined in args for jobid:'.$self->getJobid);
	}	
	$self->setArgs( $args );
	return $args;
}


=head2 burst()

=cut

sub burst {
	my $self = shift;
	my $job = $self->job();
    my $parentJobid = $self->getJobid();
	my $args = $job->getArgs();
	my $xs = XML::Simple->new(SuppressEmpty => undef, ForceArray => [ 'job' ]);
    my $jobCount = 0;
    my @childJobs;
    my $defaultClass;

    # if we have jobs to burst, go ahead and create the necessary tables for them
    # if we don't, something has already gone wrong and we should stop
    if ( defined($args->{jobs}->{job}) && @{ $args->{jobs}->{job} } ) {
    	$self->createMetajobTables();
    } else {
    	throw HeliosX::MapReduceService::BurstError("There are no jobs to burst from this metajob");
    }
    
    # if a default class was defined in the arg XML, we'll use that in making the jobs
    if ( defined($args->{class}) ) {
    	$defaultClass = $args->{class};
    } elsif ( defined($args->{defaultClass}) ) {
    	$defaultClass = $args->{defaultClass};
    }
    
    try {
    	foreach my $childArgs ( @{ $args->{jobs}->{job} } ) {
    		# should we use the job's specified class name or the default?
            my $jobClass;
    		if ( defined($childArgs->{class}) ) {
    			$jobClass = $childArgs->{class};
    		} else {
    			$jobClass = $defaultClass;
    			$childArgs->{class} = $defaultClass;
    		}
    		
    		# we have to add a few things (parentJobId) to the job's args
    		$childArgs->{parentJobid} = $parentJobid;
    		
    		# then create a new job object representing the new job
    		my $childArgsXML = $xs->XMLout($childArgs, NoAttr => 1, NoIndent => 1, RootName => undef);    #[] fix this?
    		my $childJob = TheSchwartz::Job->new(funcname => $jobClass, arg => [ $childArgsXML ] );
    		push(@childJobs, $childJob);
    	}
    	# OK, we've got all the jobs, put them in the db
    	$job->replace_with(@childJobs);        #[] does this remove the metajob???

        # OK, the jobs are in the queue, but we need to note them in the id table
        my @childJobids;
        foreach (@childJobs) {
        	push(@childJobids, [$_->jobid]);
        }

        my $d = $self->getDriver();
        HeliosX::MapReduceService::MetajobId->install_properties({
            datasource  => 'helios_metajob_ids_'.$self->getJobid(),
            columns     => ['jobid'],
            primary_key => 'jobid'
        });
        HeliosX::MapReduceService::MetajobId->bulk_insert(['jobid'], @childJobids);

    } otherwise {
    	my $e = shift;
        throw HeliosX::MapReduceService::BurstError($e->text());
    };

    $self->setJobCount(scalar @childJobs);
	return scalar(@childJobs);
}


=head1 OTHER METHODS

=head2 createMetajobTables()

=cut

sub createMetajobTables {
    my $self = shift;
    my $jobid = $self->getJobid();

    my $metajobIdTable = 'helios_metajob_ids_'.$jobid;
    my $metajobStatusTable = 'helios_metajob_status_'.$jobid;
    
    my $metajobIdTbSQL = <<METAJOBTBSQL;
CREATE TABLE $metajobIdTable (
    jobid BIGINT UNSIGNED
)
METAJOBTBSQL
    my $metajobStatusTbSQL = <<METAJOBSTATUSSQL;
CREATE TABLE $metajobStatusTable (
    jobid BIGINT UNSIGNED, 
    complete_time INTEGER UNSIGNED, 
    exitstatus SMALLINT UNSIGNED
)    
METAJOBSTATUSSQL

    my $dbh = $self->dbConnect();
    $dbh->do($metajobIdTbSQL) or throw Helios::Error::Fatal("ERROR creating metajob id table:".$dbh->errstr);     
    $dbh->do($metajobStatusTbSQL) or throw Helios::Error::Fatal("ERROR creating metajob status table:".$dbh->errstr);     
    $self->setMetajobIdTable($metajobIdTable);
    $self->setMetajobStatusTable($metajobStatusTable);
    return 1;
}


=head2 updateStatistics()

=cut

sub updateStatistics {
	my $self = shift;
    my $metajobStatusTable = $self->getMetajobStatusTable();
    my ($totalJobsComplete, $successfulJobsComplete, $failedJobsComplete);
    try {
    	my $dbh = $self->dbConnect();
        ($totalJobsComplete) = $dbh->selectrow_array("SELECT COUNT(*) FROM $metajobStatusTable");
        ($successfulJobsComplete) = $dbh->selectrow_array("SELECT COUNT(*) FROM $metajobStatusTable WHERE exitstatus = 0");
        ($failedJobsComplete) = $dbh->selectrow_array("SELECT COUNT(*) FROM $metajobStatusTable WHERE exitstatus != 0");
    } otherwise {
    	my $e = shift;
    	throw Helios::Error::DatabaseError($e->text());
    };
    $self->setCompletedJobCount($totalJobsComplete);
    $self->setSuccessfulJobCount($successfulJobsComplete);
    $self->setFailedJobCount($failedJobsComplete);
    return 1;
}


=head2 dbConnect($dsn, $user, $password)

=cut

sub dbConnect {
    my $self = shift;
    my $dsn = shift;
    my $user = shift;
    my $password = shift;
    my $options = shift;
    my $config = $self->getConfig();

    try {

        my $dbh;
        if ($options) {
            my $o = eval "{$options}";
            if ($@) { throw Helios::Error::Fatal($@);  }
            $dbh = DBI->connect_cached($dsn, $user, $password, $o); 
        } else {
            $dbh = DBI->connect_cached($dsn, $user, $password);
        }
        if ( !defined($dbh) ) { 
            throw Helios::Error::DatabaseError($DBI::errstr);
        }
        $dbh->{RaiseError} = 1;
        return $dbh;

    } otherwise {
    	my $e = shift;
        throw Helios::Error::Fatal($e->text);
    };

}


=head1 SEE ALSO

L<HeliosX::MapReduceService>, L<HeliosX::MapReduceService::Job>

=head1 AUTHOR

Andrew Johnson, E<lt>lajandy at cpan dotorgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2009 by Andrew Johnson

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
