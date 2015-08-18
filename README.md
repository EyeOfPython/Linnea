# Linnea
A compiler for the Linnea query language.

Linnea can be used to find out which clients in a network are showing unusual request patterns. 

## Example Elephant:
```
{
	timestamp≥t0-2h,timestamp≤t0, 
	nxdomain, 
	match(domain,'^[a-f0-9]{8}\.(com|info|net)$^')
}, 
{
	[client| |suffix in 'com^','info','net':[client,d1 |d0=suffix]≥2| ]≥16
}
```
Which would read as:
1.	Select all NXDOMAINS that match a specific regex.
2.	Given this set, a client has to query at least 16 domains with at least two identical 2-strings for different suffixes.

And compile to:
```
SELECT dst, COUNT(dst) AS freq
FROM (
    SELECT request, dst, d0, d1, timestamp
    FROM (
        SELECT request, dst, d0, d1, timestamp,
            COUNT(((CASE WHEN (number_1 >= 1) THEN 1 ELSE 0 END)+(CASE WHEN (number_1 >= 1) THEN 1 ELSE 0 END)+(CASE WHEN (number_1 >= 1) THEN 1 ELSE 0 END)) >=  2 OR NULL) OVER(PARTITION BY dst ORDER BY timestamp RANGE BETWEEN INTERVAL '1 hour 0 minute' PRECEDING AND INTERVAL '1 hour 0 minute' FOLLOWING) AS number_0
        FROM (
            SELECT request, dst, d0, d1, timestamp,
                COUNT(d0 = 'com' OR NULL) OVER(PARTITION BY dst,d1 ORDER BY timestamp RANGE BETWEEN INTERVAL '1 hour 0 minute' PRECEDING AND INTERVAL '1 hour 0 minute' FOLLOWING) AS number_1
            FROM (
                SELECT request, dst, d0, d1, MAX(timestamp) AS timestamp
                FROM hplDNSReplies
                WHERE
                    timestamp >= (TIMESTAMP '2015-08-10 02:00:00') - INTERVAL '2 hour 0 minute'
                    AND timestamp <= (TIMESTAMP '2015-08-10 02:00:00')
                    AND (cat='NXDOMAIN')
                    AND (REGEXP_INSTR(request,'^[a-f0-9]{8}\.(com|info|net)$')>0
)
                GROUP BY dst, request, d0, d1
            ) layer_0
        ) layer_1
    ) layer_2
    WHERE number_0 >= 10
) layer_group
GROUP BY dst
```

## Run Linnea
`python linnea.py <grammar-file> <timestamp> <groupby> <execute>`
to run Linnea from the command line. Timestamp has to be of format YYYY-MM-DD HH:MM:SS, use quotes.
If group by=1 (default), adds a surrounding aggregate that reduces the queries to client-wise.
If connection values are set properly in the config.toml and execute=1 (default 0), then the query gets executed directly.

## Language outline
1. `P_0,…,P_n` defines the program, where each P_i is a predicate layer. Starting with P_0, the entirety of the domain data is fed into P_0, which yields the remaining domains, which will be fed into〖 P〗_1, and so forth until〖 P〗_n, which will be the output of the program.
2. `{p_0,…,p_n }`  defines a predicate set; it is true iff all predicates p_0,…,p_n  are true. A predicate set forms a predicate layer.
3. For Booleans and numbers, we have the usual arithmetic and logical operators.
4. `a=b` is the string equality predicate.
5. `match(s,r)` is true when the string s matches the regular expression r.
6. `count(s,r)` counts the number of occurrences of the regular expression r in s.
7. `|g in h_0,…,h_n:p_i |` counts how often the predicate p_i holds for each g∈{h_0,…,h_n }.
8. `g in h_0,…,h_n` yields true iff i∈{h_0,…,h_n }. 
9. `[a_0,…,a_n:T|p]` finds in the current set of domain data those domains, that are in the timeframe [timestamp-T;timestamp+T]   and have the same properties as the current domain, with respect to the property names a_0,…,a_n. It then counts given those how many fulfil the predicate p. These expressions are not allowed in P_0 for performance reasons.
10. Various variables for each domain can be accessed, e.g. `domain, client, timestamp, nxdomain, d0, l0`.

## More examples
Run `python linnea.py examples/<dga>.linn` to 
```
{
	timestamp≥t_0-2h,timestamp≤t_0 
	nxdomain, 
	match(domain,'^[a-z]{5,12}\.(biz|com|info|net|org) $^')
}, 
{
	[client|true]≥25, 
	|i in 5,…,12:[client│l1=i]≥1|≥5,
	|suffix in 'com','biz','info','net','org': [client|d0=suffix]≥1|≥4,
	[client|l1=5 and d0  in 'com^','biz','info','net','org'] ≥1,  
	[client|l1=12 and d0  in 'com^','biz','info','net','org'] =0 
}
```

Which reads as:
1. Select all NXDOMAINs that match a specific regex within the last two hours.
2. Given this set, a client has to query at least 25 domains, at least 5 different 2-lengths, at least 4 different suffixes, at least one domain of length 5 with suffix com/info/net/org, and no domains of length 12 with suffix com/info/net/org.

```
{
	timestamp≥t_0-2h,timestamp≤t_0 
	nxdomain, 
	match(domain,'^[a-z]{11,23}\.com$^')
}, 
{
	[client|true]≥20, 
	[client|count(d_1 ,^'-')=1] ≥9,  
	[client│count(d_1 ,^'-')=2] ≥9 
}
```

Which reads as:
1. Select all NXDOMAINS that match a specific regex.
2. Given this set, a client has to query at least 25 domains, at least 20 domains containing exactly one hyphen, and at least 20 domains containing exactly 2 hyphens.

