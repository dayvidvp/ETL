# 04/09/2022

- ~~added tray.deleted = 0 to the query for the tray list~~
- ~~cache the builds from the already exported builds and compare them with the builds exported from spmi. Process only the ones needed:~~
  - delete the builds that are not in the spmi export, but are in the cache
  - add the builds that are in the spmi export, but are not in the cache
- hard delete tray
- ~~added latest packing per tray to the database~~
- similar componets, add the GR articles to the list
- ~~add description to the tpi check because erpreferencenumber is niet uniek~~
- ~~changed component cache to pandas dataframe for speedup~~
- ~~fixed import for articles and tpi's with a quote in the description~~
