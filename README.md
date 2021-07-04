# array_hash

This operator computes a distribution-independent parallelized hash signature of a SciDB array. The input can be any stored array name or afl expression returning an array. The output is a 12-byte hash signature and a cell count in dataframe form.

array_hash has an avalanche property such that small changes to the input cause a large change in the result. For example:
```
$ iquery -aq "array_hash(build(<val:string>[i=1:1], 'the quick brown fox'))"
{inst,seq} data_hash,count
{0,0} '20e3e8e320e3e8e320e3e8e3',1

$ iquery -aq "array_hash(build(<val:string>[i=1:1], 'The quick brown fox'))"
{inst,seq} data_hash,count
{0,0} 'a2869654a2869654a2869654',1
```

One use case is to check that two arrays have the same contents; often to verify data integrity. For instance, say you calculate a large set of Regenie-gene results and save them to S3 via Bridge. You can verify the save like so:
```
$ iquery -aq "array_hash(GENE_ASSOC_LARGE_REGENIE_GENE)"
{inst,seq} data_hash,count
{0,0} 'e012913c3a7184bf27e18d82',12262856

$ iquery -aq "array_hash(xinput('s3://bb.internal/association_data/public.GENE_ASSOC_LARGE_REGENIE_GENE'))"
{inst,seq} data_hash,count
{0,0} 'e012913c3a7184bf27e18d82',12262856
```

## Specifics on behavior

The following things affect the hash; changing them will alter the result:
 * array data - adding or removing cells, changing a value, setting a value to null or changing null code
 * cell positions and number of dimensions (except dataframes)
 * changing attributes to dimensions or vice-versa
 * changing datatype size - like casting from a `float` to a `double`. Note that only the size and binary sequence is considered. Thus a `1` is considered the same in `uint32` versus `in32` and also a `1` in `uint8` happens to be the same as a `bool` value of `true`.
 * the order of attributes values

And when we say "will alter the result" we mean "with overwhelmingly high probability." Hash collisions are always theoretically possible.

And on the other hand, the following things do NOT affect the hash:
 * array metadata: att,dim names, whether an attribute is allowed to be nullable, dimension upper bounds
 * chunk sizes
 * SciDB cluster topology - number of instances
 * array distribution or residency
 * order of rows in a dataframe. In fact `array_hash( X )` is always the same as `array_hash( flatten (X))`

To reiterate, cell values and coordinates affect the result; but the cell-to-instance distribution does not.

## Empty Arrays

For these the returned hash is `null` and the returned count is `0`.

## Algorithm

For each array cell we assemble the tuple into a single buffer, with dimensions first, then attributes. Each attribute is prepended by the 1-byte null-code. We then `murmuhash32` the cell. Having hashed each cell, we combine these quantities using 3 operations:
 * sum
 * product modulo a large prime
 * xor

These 3 operators are associative and commutative and we use that to merge the tuple hashes in parallel. Finally we concatenate these three 4-byte quantities into a single 12-byte result. That's the returned `hash`. This is why the hash of a single-cell array looks like a repeating pattern. This also means that computed hash values can be "merged" together with other hashes if desired.

## Big Array Timings

Generally performs OK. Perhaps the `%` stuff can be sped up. For the very large UK_BIOBANK genotype arrays we have the following timings on one box:
```
UK_BIOBANK.GENOTYPE:
16.25 minutes per 1M variants
8 hours for UK_BIOBANK.GENOTYPE with info_score >= 0.8 (~ 29M variants)
24 hours for UK_BIOBANK.GENOTYPE full (~ 90M variants)
```
Maybe needs some more burst magic.

```
UKB_200K_WES.GENOTYPE for comparison:
7 minutes per 1M variants
2 hours for UKB_200K_WES.GENOTYPE (~18M variants)
```
