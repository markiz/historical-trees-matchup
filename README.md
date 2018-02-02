# Historic trees with temporal data

See [post](historic_trees.md) for context.

## Preparing data

```
ruby ./00_prepare_data.rb -o small_set.dat --initial-inserts 100 --update-num 50 --reads-per-update 200
ruby ./00_prepare_data.rb -o medium_set.dat --initial-inserts 500 --update-num 500 --reads-per-update 200
ruby ./00_prepare_data.rb -o small_set.dat --initial-inserts 100 --update-num 5000 --reads-per-update 200
```

## Running tests

Check the `test_runs.yml` file for db config, then run:

```
ruby ./run_all.rb
```

Results will be output to `test_results.yml`
