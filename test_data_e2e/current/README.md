# Test data description

Two versions of the data are provided:
   - `full_dataset`, composed of 17 patients (11 males, 6 females)
   - `alternative`, from which patient PAT2 (male) and all associated data (diagnosis, 
     biosource, biomaterial, radiology) has been removed.

The dataset is designed to allow testing of various subselections in the query builder of Glowing 
Bear. More details about specific entities are provided below.

### Diagnosis entity


|            | Full dataset |     |       | Alternative |     |        |
|------------|--------------|-----|-------|-------------|-----|--------|
| tumor_type | M            | F   | TOTAL | M           | F   | TOTAL  |
| ---------- | ------------ | --- | ------| ------------| --- | ------ |
| TC only    | 1            | 4   | 5     | 1           | 4   | 5      |
| NH only    | 8            | 2   | 11    | 8           | 2   | 10     |
| both       | 2            | 0   | 2     | 1           | 0   | 1      |
| ---------- | ------------ | --- | ------| ------------| --- | ------ |
| TOTAL      | 11           | 6   | 17    | 10          | 6   | 16     |


### Radiology entity


|            | Full dataset |     |        | Alternative  |     |        |
|------------|--------------|-----|--------|--------------|-----|--------|
| body_part  | M            | F   | TOTAL  | M            | F   | TOTAL  |
| ---------- | ------------ | --- | ------ | ------------ | --- | ------ |
| legs only  | 4            | 2   | 6      | 4            | 2   | 6      |
| torso only | 3            | 1   | 4      | 3            | 1   | 4      |
| both       | 2            | 0   | 2      | 1            | 0   | 1      |
| ---------- | ------------ | --- | ------ | ------------ | --- | ------ |
| TOTAL      | 9            | 3   | 12     | 8            | 3   | 11     |
