#!/usr/bin/awk -f
BEGIN {
	min_val = 0;
	min_val_defined = 0;
	max_val = 0;
	max_val_defined = 0;
}
{
	if ($1 > max_val || max_val_defined == 0) {
		max_val = $1;
		max_val_defined = 1;
	}
	if ($1 < min_val || min_val_defined == 0) {
		min_val = $1;
		min_val_defined = 1;
	}
}
END {
	print "Max disk allocation = ", (max_val - min_val) / 1024, "GiB";
}
