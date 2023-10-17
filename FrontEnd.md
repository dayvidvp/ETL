#Front-End application masterdata cleanup

##Introduction

Standalone application that reads data from the collected master from the spmi servers and processes this data to create a transform that will output a cleaned and structured masterdata file/database.

##Data aquisition
The data is already available at STRDVPDBSQL01/Masterdata_steam.
data layout to be discussed and is not yet available.

###Application specifications

##general features

- fast and easy to use :)
- Possible for multiple users to use the application at the same time (lock mechanism)

##Data processing

The data is processed in the following steps:

- See the list of current non processed components
- After a component or componentname is selected this component is now markes as beeing processed
- All the relevant data is displayed to the user
  - All similar componentid's (with percentage?) and their relevant componentname's
  - All similar componentnames (with percentage?) and their relevant componentid's
  - Possibly the number of counts per id in a tpi
  - Componetname and componentid's should not already be discarded
- When one of the componentid's is selected as master the other similiar componentid's are set as not available
- Then the componentname can be selected as master and the other similiar componentnames are set as not available
- Then the relevant pictures need to be shown of all the componentid's (seperate then the actual chosen componentid), so the user can select one or more pictures to add to the component id. The other pictures are discarded.
- All the relevant transformation data needs to be stored in the database


##Output

The data remains as is, but all the transformations are recorded.
There needs to be periodic exports of the final data to make mock exports to 3P.
