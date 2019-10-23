-- 门急诊就诊记录结构表（获取前31天内主要诊断疾病代码 主要疾病代码top5）
select * from 
(select substr(MD_DIS_CODE,0,5),ORGAN_CODE,count(1) from MedicalRecord
where SEE_DOC_DT <= sysdate
and SEE_DOC_DT>(sysdate-31)
group by substr(MD_DIS_CODE,0,5),ORGAN_CODE
order by count(1) desc)
where rownum<6