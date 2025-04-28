delete from {{ params.target }}
WHERE 
	data_hora_inicio >= CURRENT_DATE - INTERVAL '60 days' 
	or data_hora_fim >= CURRENT_DATE - INTERVAL '60 days';
	
insert into {{ params.target }}(
	*
)
select 
		*,
		now() - INTERVAL '3 hours'	--data_atualizacao
from {{ params.source }};