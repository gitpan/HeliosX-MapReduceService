package HeliosX::MapReduceService;

use 5.008;
use base qw(HeliosX::ExtLoggerService);
use strict;
use warnings;

use Helios::Error;
use HeliosX::LogEntry::Levels qw(:all);

use HeliosX::Metajob;

our $VERSION = '0.02_0911';

=head1 NAME

HeliosX::MapReduceService - Alternative Helios::Service class to provide simple map/reduce operations

=head1 SYNOPSIS

  use HeliosX::MapReduceService;
  blah blah blah

=head1 DESCRIPTION

This module is not yet complete, thus the documentation is not yet complete.

The idea here is to submit a metajob to a Helios collective, have the metajob burst into as many 
jobs as necessary to complete the task (map), wait for them to complete, and then run any final, 
cleanup tasks (reduce) as may be necessary.

Normally, a Helios service is simply a subclass of the Helios::Service class, and normally the 
only method a service class really needs to implement is the run() method,  

=head1 SETUP METHODS

=cut

sub MaxTimeWindow { return undef; }
sub StatisticsUpdateInterval { return 60; }
sub JobClass { return undef; }

=head1 METHODS

=head2 work()

Normal Helios::Service subclasses do not need to override work(), but since 
HeliosX::MapReduceService has an expanded view of Helios jobs and what Helios 
should with them, it has to override the work() class method.

=cut

sub work {
	my $class = shift;
	my $schwartzJob = shift;
	my $metajob = $class->job_class() ? $class->job_class()->new($schwartzJob) : HeliosX::Metajob->new($schwartzJob);
	my $rc;
	my $args;

	# instantiate the service class into a worker
	my $self = $class->new();

	try {
		$self->prep();
		$metajob->debug( $self->debug );
		$metajob->setConfig($self->getConfig());
		$args = $metajob->parseArgs();
	} catch Helios::Error::InvalidArg with {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, "Invalid arguments: ".$e->text);
		$metajob->failedNoRetry($e->text);			
		exit(1);
	} catch Helios::Error::DatabaseError with {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, "Database error: ".$e->text);
		$metajob->failed($e->text);
		exit(1);
	} otherwise {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, "Unexpected error: ".$e->text);
		$metajob->failed($e->text);
		exit(1);
	};

	# run the prerun() method for this metajob
	try {
		$rc = $self->prerun($metajob);		
	} otherwise {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' prerun failure; uncaught exception: '.$e->text);
	};
	if ($self->debug) {	$self->logMsg($metajob, LOG_DEBUG, 'Metajob '.$metajob->getJobid.' prerun() return code: '.$rc);	}
	# check return code
	if ( defined($rc) && $rc != 0) {
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' prerun failed with nonzero status: '.$rc);
		exit(1);
	}
		
	#[]? run should burst the jobs apart and then wait until they complete
	try {
		$rc = $self->run($metajob);
	} otherwise {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' run failure; uncaught exception: '.$e->text);
	};
	if ($self->debug) {	$self->logMsg($metajob, LOG_DEBUG, 'Metajob '.$metajob->getJobid.' run() return code: '.$rc);	}
	# check return code
	if ( defined($rc) && $rc != 0) {
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' run failed with nonzero status: '.$rc);
		exit(1);
	}

	# run the postrun() method for this metajob
	try {
		$rc = $self->postrun($metajob);		
	} otherwise {
		my $e = shift;
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' postrun failure; uncaught exception: '.$e->text);
	};
	if ($self->debug) {	$self->logMsg($metajob, LOG_DEBUG, 'Metajob '.$metajob->getJobid.' postrun() return code: '.$rc);	}
	# check return code
	if ( defined($rc) && $rc != 0) {
		$self->logMsg($metajob, LOG_ERR, 'Metajob '.$metajob->getJobid. ' postrun failed with nonzero status: '.$rc);
		exit(1);
	}

	# just a note:  metajobs don't support OVERDRIVE mode (though the jobs burst from them may)
	# there's really no need
	#[]? or is there?
	exit(0);
}


=head2 prerun()

=cut

sub prerun {
	
}


=head2 run($metajob)

=cut

sub run {
	my $self = shift;
	my $metajob = shift;

    try {
        $self->burstJob($metajob);
        $self->waitForJobs($metajob);
        return 0;
    } catch HeliosX::MapReduceService::BurstError with {
    	my $e = shift;
    	$self->logMsg($metajob,LOG_ERR,'METAJOB BURSTING ERROR: '.$e->text());
        return 1;
    } otherwise {	
    	my $e = shift;
    	$self->logMsg($metajob,LOG_ERR,'Unexpected error: '.$e->text());
        return 1;
    };

}


=head2 postrun($metajob)

=cut 

sub postrun {
    my $self = shift;
    my $metajob = shift;
    $self->completedJob($metajob);
    return 0;
}


=head2 waitForJobs($metajob)

=cut

sub waitForJobs {
	my $self = shift;
	my $metajob = shift;
	my $windowClose = undef;
    if (defined($self->max_time_window()) ) {
        my $windowClose = time() + $self->max_time_window();   
    }
    
    do {
    	$metajob->updateStatistics();
    	$self->logMsg($metajob, LOG_INFO, 'Processing. Total jobs:'.$metajob->getJobCount().
    	       ' Completed:'.$metajob->getCompletedJobCount(). ' Successful:'.$metajob->getSuccessfulJobCount().
    	       ' Failed:'.$metajob->getFailedJobCount()
    	);
    	sleep $self->monitor_interval();
    } until ( (defined($windowClose) && time() > $windowClose) || $self->getCompletedJobCount() == $self->getJobCount() );
    
    if ( defined($windowClose) && time() > $windowClose ) {
    	$self->logMsg($metajob, LOG_ALERT, 'Processing time window expired.  Total jobs:'.$metajob->getJobCount().
               ' Completed:'.$metajob->getCompletedJobCount(). ' Successful:'.$metajob->getSuccessfulJobCount().
               ' Failed:'.$metajob->getFailedJobCount()
    	);
    } else {
        $self->logMsg($metajob, LOG_INFO, 'Processing completed.  Total jobs:'.$metajob->getJobCount().
               ' Completed:'.$metajob->getCompletedJobCount(). ' Successful:'.$metajob->getSuccessfulJobCount().
               ' Failed:'.$metajob->getFailedJobCount()
        );
    }
    return 1;
}


1;
__END__


=head1 SEE ALSO

L<Helios::Service>, L<HeliosX::ExtLoggerService>, L<HeliosX::Metajob>

=head1 AUTHOR

Andrew Johnson, E<lt>lajandy at cpan dotorgE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008-9 by Andrew Johnson

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.0 or,
at your option, any later version of Perl 5 you may have available.


=cut
