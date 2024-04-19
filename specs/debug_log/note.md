Why after running for a while, data pushed to bitcoin layer goes from 1.39kb, to 35.83kb, to 69.87kb. What is the reason for this jump?

1. It jumps from pushing (1, 3) blocks to (1, 79) blocks

the system recognizes roll - ups blocks in store, but why not submitting? because it is stuck in submit attempt loop

that was due to unset private and internal private key

2. Witness field data is empty? If submitted, it should have been something else

```
[0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0]
```

because it was fetching from height 0, I need to set another bitcoin start height

3. engine is fetching old btc blocks first from last test run, which disrupt btc height to new test runs
* data pushed to bitcoin needs to define version