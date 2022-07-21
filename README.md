gcstool
-------

A small (prerelease) tool for creating and searching [Golomb Compressed Sets][1].


### What?

Golomb Compressed Sets are similar to [Bloom filters][2] - they're space-efficient
data structures that let you test whether a given element is a member of a set.

Like Bloom filters, they have a controllable rate of false-positives - they may
consider an element a member of a set even if it's never been seen before - while
having no false negatives.  If it's not on the list, the GCS hasn't seen it.


### Why?

Let's illustrate with a real-world problem: checking against leaked password lists.

Say you have a copy of [pwned-passwords-2.0.txt][3], and you want to check
your users passwords against the list when they sign up.  Unfortunately it's
nearly 30GB, and not in a format particularly suited for searching.

You cut it down quite a bit simply by converting the SHA1 hashes in it to
binary, and even truncating them in the process, but even being really aggressive
and spending just 6 bytes per entry (for a false-positive rate of 1 in 500,000)
only gets you down to 3 GB.

Let's see what this tool can do:

    # Use precomputed hashes directly with -H hex
    % gcstool -H hex create -p 500000 pwned-passwords-2.0.txt pwned-passwords-2.0-p500k.gcs
    (5 minutes and 3.8 GiB of RAM later)
    % du -h pwned-passwords-2.0-p500k.gcs
    1.2G    pwned-passwords-2.0-p500k.gcs

An equivalent Bloom filter would consume [1.6 GiB][4].  Not too shabby.

But 1 in 500,000 passwords being randomly rejected for no good reason is a bit crap.
Your users deserve better than that, surely.  You could double-check against the
[pwned passwords API][5], but then we're leaking a little bit about our users
passwords to a third party and that makes me itch.  What if we could just up the
error rate so it's negligible?

    % gcstool -H hex create -p 50000000 pwned-passwords-2.0.txt pwned-passwords-2.0-p50m.gcs
    (5 minutes and 3.8 GiB of RAM later)
    % du -h pwned-passwords-2.0-p50m.gcs
    1.6G    pwned-passwords-2.0-p50m.gcs

Contrast this with a [2.15 GiB Bloom filter][5].

We can now query a database for for known passwords:

    % gcstool query pwned-passwords-2.0-p50m.gcs
    Ready for queries on 501636842 items with a 1 in 50000000 false-positive rate.  ^D to exit.
    > password
    Found in 0.3ms
    > not a leaked password
    Not found in 1.7ms
    > I love dogs
    Found in 0.7ms
    > I guess it works
    Not found in 0.1ms

Yay.

Integrating it into your website is left as an exercise right now.  Eventually I'll
factor this out into a library and sort out a Rubygem.


### How?

Golomb Compressed Sets are surprisingly simple:

Choose a false positive rate `p`.  For each of your `n` elements, hash to an integer
`mod pn`.  Then sort the list of integers, and convert each entry to a diff from the
previous one, so you end up with a list of offsets.

This is the set - just a list of offsets.  You can prove to yourself that this works
because you've distributed your n items in n*p buckets, so a bucket has just a
1-in-p probability of being filled by accident by any given item.

Now it's just a matter of storing each offset efficiently.  We use a subset variant of
[Golomb coding][6] called Rice coding - divmod by p, store the quotient in unary
(0 = 0, 1 = 1, 2 = 11, 3 = 111, etc), and store the remainder in log2(p) bits.

Then we simply store an index indicating what and where every `n`th element is so we
can jump in the middle of the set to find a random item.  In this version of the GCS,
we simply store pairs of 64-bit integers indicating the number and the offset.

More complex index encoding is possible to make it smaller, but since a default index
for even 500 million items is just 16MB it barely seems worth the effort.


[1]: http://giovanni.bajo.it/post/47119962313/golomb-coded-sets-smaller-than-bloom-filters
[2]: https://en.wikipedia.org/wiki/Bloom_filter
[3]: https://haveibeenpwned.com/Passwords
[4]: https://hur.st/bloomfilter/?n=501652074&p=500000&m=&k=
[5]: https://hur.st/bloomfilter/?n=501652074&p=50M&m=&k=
[6]: https://en.wikipedia.org/wiki/Golomb_coding
