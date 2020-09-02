I've been trying to define a working hierarchy for the BLS Employment data, but I've run into a roadblock. I will take a sample of codes for Mining to explain what happens. Ideally, we would have this structure, considering all the industries in the group:

```
(10000000) Mining and Logging
|_ (10113300) Logging
|_ (10210000) Mining, Quarrying, and Oil and Gas Extraction
    |_ (10211000) Oil and Gas Extraction
    |_ (10212000) Mining (except Oil and Gas)
        |_ (10212100) Coal Mining [*]
        |_ (10212200) Metal Ore Mining [*]
    |_ (10213000) Support Activities for Mining
        |_ (10213111) Drilling Oil and Gas Wells [*]
        |_ (10213112) Support Activities for Oil and Gas Operations [*]
```

My plan was to keep the deepest levels only (marked with [*]), so I could define the parent levels around them. But not all the states have each deeper level. For Mining, California and Wyoming show the most information:

```
CALIFORNIA
(10000000) Mining and Logging: 22,400
|_ (10113300) Logging: 1,800
|_ (10210000) Mining, Quarrying, and Oil and Gas Extraction: 20,600
    |_ (10211000) Oil and Gas Extraction: 4,600
    |_ (10212000) Mining (except Oil and Gas): 5,900
    |_ (10213000) Support Activities for Mining: 10,100
```

Here, I would have deleted all the levels that have information, since the deeper levels I wanted to keep are not available.

```
WYOMING
(10000000) Mining and Logging: 20,900
|_ (10210000) Mining, Quarrying, and Oil and Gas Extraction: 20,900
    |_ (10211000) Oil and Gas Extraction: 2,900
    |_ (10212000) Mining (except Oil and Gas): 8,000
        |_ (10212100) Coal Mining: 5,300
    |_ (10213000) Support Activities for Mining: 10,000
        |_ (10213112) Support Activities for Oil and Gas Operations: 7,800
```

Here it is possible to infer the missing values, but it's a rare case, because there are 29 states that only have "Mining and Logging" without any children, and the state of Delaware doesn't have "Mining and Logging" at all. This happens across all industry groups and years. We can still take any of the available time series, by State and Industry and generate visualizations, even compare many of them if they're available at the same industry level.