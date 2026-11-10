Changelog
=========

Development
-----------

* Add a keyword-only bigquery_client argument to FlexValueRun. BigQuery runs use that client, and its project, instead of building their own.

2.0.8
-----


* Update documentation to fix incorrect link

2.0.7
-----


* Add support for use_value_curve_name_for_join to command line

2.0.6
-----


* Update requirements in setup.py.
* Update tests.
* Remove legacy files from flexvalue 1.0.
* Add an option to join on value_curve_name.

2.0.5
-----


* When processing the metered load shape, only copy the elec load shape if we don't process it.

2.0.4
-----


* Add support for the case that the load shape is already the right format.

2.0.3
-----


* Include sql files in built package

2.0.2
-----


* Change Jinja template load approach

2.0.1
-----


* Change how Jinja loads templates

2.0.0
-----


* Rewrite FLEXvalue to use sql calculations instead of pandas. Supports Google BigQuery and PostgreSQL.

0.4.6
-----


* Add SCG to therms constants.
* Pin numpy version.

0.4.5
-----


* Fix output data type bug 

0.4.4
-----


* Iterate through projects to fix memory issues

0.4.3
-----


* get_time_series_results() now returns an interator, to prevent memory errors

0.4.2
-----


* Bump to fix failed deploy

0.4.1
-----
* Bump to fix failed deploy


0.4.0
-----


* Add time series outputs

0.3.1
-----


* Revert sqlalchemy to older version due to bug with >=1.4.0.

0.3.0
-----


* Update acc version and variable naming related to it.
* Update readme related to acc database information.
* Add additional information and sources to the collab notebook pertaining to load shapes and acc.

0.2.9
-----

* Fix tests to not rely on a real db. 
* Update cet_scan_compare to take in 'Non_Res'.

0.2.8
-----

* Update function for parsing CET output results.

0.2.7
-----

* Add function for directly parsing CET output results.

0.2.6
-----

* Fix plotting error.

0.2.5
-----

* Adjust therms_profile_adjustment based on additional empirical testing.

0.2.4
-----

* Update error log on missing load shape.

0.2.3
-----

* Update so user-inputs deer load shapes don't require utility prefix. 
* Point to versioned folder of the 2020 database (v1).

0.2.2
-----

* Update CET comparison tool with new utility-based DEER load shapes.

0.2.1
-----

* Add test for parse_cet_output
* Update the cet output parser so it can more easily discover files.

0.2.0
-----

* Renamed columns on output table and separated out totals.
* Add CET comparison code.
* Add git workflow for testing.
* Update in-line documentation.
* Update default filepaths for input and output.

0.1.0
-----

* Add `acc_electricity_utilities_climate_zones` table for easy access.
* Rename `deer_ls` to `deer_load_shapes`.

0.0.1
-----

* Initial release
