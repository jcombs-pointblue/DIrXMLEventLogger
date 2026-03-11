SELECT *
FROM public.dxmlevent
WHERE "eventjson" -> 'attributes' -> 'last_name' ->> 'value' = 'Man';