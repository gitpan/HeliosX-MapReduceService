use 5.008;
use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    NAME              => 'HeliosX::MapReduceService',
    VERSION_FROM      => 'lib/HeliosX/MapReduceService.pm', # finds $VERSION
    LICENSE           => 'perl',
    PREREQ_PM         => {
                            Helios => 2.00,
                            HeliosX::ExtLoggerService => 0.02,
                         }, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/HeliosX/MapReduceService.pm', # retrieve abstract from module
       AUTHOR         => 'Andrew Johnson <lajandy@cpan.org>') : ()),
);
