awk '
BEGIN { RS=">"; ORS="" }
NR > 1 {
  split($1, header_parts, "_comp")
  locus = header_parts[1] "_comp"
  if (!(locus in seen)) {
    seen[locus] = 1
    print ">" $0
  }
}
' ${1} > $(basename ${1})
