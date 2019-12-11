# Test data set for sample based subselections

This test data set is intended for testing intersection of patient groups
for several diagnosis and sample properties.

The folder also contains test data for cBioportal in the NGS folder.
The data include the following samples:

- BIOS1T_BIOM1T (tumor) and BIOS1N_BIOM1N (normal) for patient PAT1,
  for which both mutation (.maf) and CNA (.seg) data are available
  
- BIOS3T_BIOM3T (tumor) and BIOS3N_BIOM3N (normal) for patient PAT3,
  for which only mutation (.maf) data are available


To translate the set for loading with [transmart-copy], run the following commands:

```bash
sources2csr dropzone /tmp/csr_test_logic config
csr2transmart /tmp/csr_test_logic /tmp/transmart_test_logic
```

The data should be written to `/tmp/transmart_test_logic`.
To load it into TranSMART using [transmart-copy], run:

```bash
export PGUSER=biomart_user PGPASSWORD=biomart_user PGPORT=5432
java -jar transmart-copy.jar -d /tmp/transmart_test_logic
```

Change the variables according to your database settings.
Check [transmart-copy] for instructions on how to get and use the tool.


[transmart-copy]: https://github.com/thehyve/transmart-core/tree/dev/transmart-copy
