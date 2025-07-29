select
name,
'/grphome/grp_geode/raw_sequences_all/' || r1,
'/grphome/grp_geode/raw_sequences_all/' || r2,
'/grphome/grp_geode/payton/genbank-odonata-mt-genomes/' || case when species_match is not null then species_match
when genus_match is not null then genus_match
when family_match is not null then family_match
when suborder_match is not null then suborder_match
end || '.gb'
from (
	with taxonomy as (select mg.genbank_accession, vt.family_id, vt.genus_id, vt.species_id from mito_genome mg left join v_taxonomy vt on mg.species_id = vt.species_id)
	select *,
	(select genbank_accession from taxonomy where r2.species_id = taxonomy.species_id) as species_match,
	(select genbank_accession from taxonomy where r2.genus_id = taxonomy.genus_id limit 1) as genus_match,
	(select genbank_accession from taxonomy where r2.family_id = taxonomy.family_id limit 1) as family_match,
	case when r2.family_id in (select id from taxonworks where parent_id in (select id from taxonworks where parent_id = 707406)) then 'MT584123.1'
	when r2.family_id in (select id from taxonworks where parent_id in (select id from taxonworks where parent_id = 707408)) then 'KF718295.1' 
	else 'MT584123.1' end as suborder_match
		from rapid2 r2
	)
	order by name
	;
