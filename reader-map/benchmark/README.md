To reproduce results, run with:

```shell
cargo build --release
rm results.log
for w in 1 2 4; do
  for d in uniform skewed; do
    for r in 1 2 4 8 16 32; do
      target/release/concurrent-map-bench -r $r -w $w -d $d -c | tee -a results.log;
    done;
  done;
done
for e in 2 4 8 16 32 64 128 256 512 1024; do
  for d in uniform skewed; do
    target/release/concurrent-map-bench -r 1 -w 1 -d $d -e $e | tee -a results.log;
  done;
done
R -q --no-readline --no-restore --no-save < plot.r
```
