### Data aqcuisition

---

- [X]: # For all spmi servers get all the active customers
- [X]: # For every customer get all the active tpi's in the system
- [X]: # For every tpi get all the active components in the system, check if the component was already processed, if not process it and log it
- [X]: # For every component get the Manufaturer, ComponentIdentifier, ComponentName, Udicodes, TPI relation data, processinstruction data and disassembly data
- [X]: # Store all the data in a database (STRDVPDBSQL01/Masterdata_steam)

### Pre processing

- [X]: # Create a query to extract all the data from the database and put in an pandas dataframe
- [X]: # Store the data in a csv file for backup and easy access

### Manufacters

---

- [X]: # Get all the unique distinct manufacturers from the dataframe
- [X]: # Get all the similiar manufacturers from the dataframe and put them in a csv file
- [X]: # Get a list of all the manufacturers with only one component

Remarks:

- The manufacturers linked to a component in a tpi could be not active in the manufacturers table
  -- This means that the manufacturer and the description do not have a unique key to fallback to and we cannot use the objectno in the manufacturer table to find the manufacturer
- The link back will need to be done on the description

### ComponentIdentifiers

---

- [X]: # Get all the unique distinct component identifiers from the dataframe
- []: # use the article_hash to link back

###Similar ComponentIdentifiers

---

    - [X]: # We need to filter the data before processing
        --Filter components that have articleid= ['Unknown', 'unk', 'Unknown', 'NO NUMBER', 'nan']
        -- Components starting with 'GR' and 'IR'
        -- Components with less then 3 characters

To get the similar compid's we first make 2 new dicts that are a copy of each other with as key the article hash and the compid
Then we can pop the compid's that are the same from the dicts and compare the rest of the dicts

### ComponentNames

---

Every time a component id is processed and compared against all the other component id's we check if the component name against all the other component names in the lookup table.
If they math we add the component id of the found components to the list of similar component id's.

### Pre processing cleanup

It is advisable to make a different dataset for Smith and nephews and johnson & johnson. Because the data is already cleaned.

### Open questions

- Will we change the componentid of loanfirm set components? I assume not because they are needed for interface data.
- Will we change the componentname of loanfirm components? They are not needed for the interface data (not veryfied)
  -- If these components are not changed then we can set these to master and not use them in processing (26K components off 85k)


 ATH_Urétrotome de Saxe Olympus 1  - 2 assembly normally WP = 6/9 2 comp (7,8) (10,11)
 ATH_ORL Drains PP 1 -- dubbele assembly na elkaar   WP= 14/15 3 comp (16,17,18)
