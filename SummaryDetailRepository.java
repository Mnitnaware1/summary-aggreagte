package com.mastercard.mcbs.pbrc.services.repository;

import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.CHARGE_DETAIL;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.ELEMENT_DETAIL_NOT_FOUND_MSG;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.FIELD_DETAILS;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.RECORD_NOT_FOUND;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.SELECT_CLAUSE;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.COMMA;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.GROUP_BY_COLUMNS;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.AGGREGATE_COLUMNS;
import static com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants.SUMMARY_TRACE_ID;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import com.mastercard.mcbs.pbrc.services.constant.ApplicationConstants;
import com.mastercard.mcbs.pbrc.services.exception.ResourceNotFoundException;
import com.mastercard.mcbs.pbrc.services.model.ElementMappings;
import com.mastercard.mcbs.pbrc.services.model.SummaryModel;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Repository
public class SummaryDetailRepository {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	/**.
	 * @param summaryModel .
	 * @param totalRecords .
	 * @param roleName .
	 * @return .
	 * @throws ResourceNotFoundException .
	 */
	public List<Map<String, Object>> findPageableRecords(SummaryModel summaryModel, int totalRecords, String roleName) throws ResourceNotFoundException {
		log.debug("Summary Details {} for given role are {}", summaryModel, roleName);
		return getElementMappingDetails(roleName, summaryModel, totalRecords);
	}

	private String buildPageableQueryString(List<ElementMappings> elementMappingDetails, SummaryModel summaryModel, int totalRecords) {

		log.info("BuildPageableQueryString with data {} and total record per page want '{}'", summaryModel, totalRecords);
		Object[] elements = elementMappingDetails.stream().map(ElementMappings :: getDataElementNam).toArray();
		Object[] asFields = elementMappingDetails.stream().map(ElementMappings :: getAsfieldsTxt).toArray();
		String selectQuery = buildSelectQuery(elements, asFields);
		StringBuilder selectClause = new StringBuilder();
		selectClause.append(SELECT_CLAUSE).append("COUNT(*) OVER() as totalRecords, ").append(selectQuery).append(" FROM bcx_owner.charge_detail cd WHERE cd.chrg_dt = to_date('")
		.append(summaryModel.getInvoiceDate()).append("','MM-DD-YYYY')");
		String whereClause = buildAndClauseString(summaryModel);
		String orderClause = " ORDER BY chrg_dt desc" + " offset nvl(" + summaryModel.getPage() + "-1,1)*" + totalRecords
				+ " rows fetch next " + totalRecords + " rows only";
		log.info("DB call with pageable query string '{}{}{}'", selectClause, whereClause, orderClause);
		return selectClause.append(whereClause).append(orderClause).toString();
	}

	/** This method fetch the element mapping details for given role.
	 * @param totalRecords .
	 * @param summaryModel .
	 * @param icaListParam SqlParameterSource
	 * @throws ResourceNotFoundException **/
	private List<Map<String, Object>> getElementMappingDetails(String roleName, SummaryModel summaryModel, int totalRecords) throws ResourceNotFoundException {
		log.info("Fetch elementMappingDetails with role {} ", roleName);
		StringBuilder sqlQuery = new StringBuilder("SELECT table_nam ,enable_sw,data_element_nam,role_nam,asfields_txt,disp_seq ,deflt_disp");
		sqlQuery.append( "  FROM bcx_owner.element_mapping WHERE table_nam=").append(CHARGE_DETAIL)
		.append(" AND role_nam=?");
		log.info("DB query to fetch element mapping details {}",sqlQuery);
		List<ElementMappings> elementMappings = jdbcTemplate.query(sqlQuery.toString(), new Object[] {roleName},
				new BeanPropertyRowMapper<ElementMappings>(ElementMappings.class));
		if (elementMappings.isEmpty()) {
			log.info("{} : {}/query above.", RECORD_NOT_FOUND, ELEMENT_DETAIL_NOT_FOUND_MSG);
			throw new ResourceNotFoundException(ELEMENT_DETAIL_NOT_FOUND_MSG);
		}
		List<Map<String, Object>> summaryResponse = jdbcTemplate.queryForList(buildPageableQueryString(elementMappings, summaryModel, totalRecords));
		if(CollectionUtils.isNotEmpty(summaryResponse)) { 
			Map<String, Object> fieldDetails = new HashMap<>(); 
			fieldDetails.put(FIELD_DETAILS, elementMappings);
			summaryResponse.add(0,fieldDetails); 
		}
		return summaryResponse;
	}

	/** This method build select query for element_mapping fields. **/
	private String buildSelectQuery(Object[] elements, Object[] asFields) {
		StringBuilder stringBuilder = new StringBuilder();
		if (elements.length == asFields.length) {
			for (int i = 0; i < asFields.length; i++) {
				Object asField = asFields[i];
				Object detailField = elements[i];
				stringBuilder.append(detailField).append(ApplicationConstants.AS).append("\"").append(asField).append("\"");
				if (i != asFields.length - 1) {
					stringBuilder.append(ApplicationConstants.COMMA);
				}
			}
			return stringBuilder.toString();
		}
		return "*";
	}

	private String buildAndClauseString(SummaryModel summaryModel) {
		String andClause = "";

		if (StringUtils.isNotEmpty(summaryModel.getBillingEvent())) {
			andClause += " AND cd.bill_event_id ='" + summaryModel.getBillingEvent() + "'";
		}
		if (StringUtils.isNotEmpty(summaryModel.getInvoiceNumber())) {
			andClause += " AND cd.inv_num ='" + summaryModel.getInvoiceNumber() + "'";
		}
		if (StringUtils.isNotEmpty(summaryModel.getFeederType())) {
			andClause += " AND cd.event_src_nam ='" + summaryModel.getFeederType() + "'";
		}
		if (StringUtils.isNotEmpty(summaryModel.getInvoiceIca())) {
			andClause += " AND cd.inv_ica ='" + summaryModel.getInvoiceIca() + "'";
		}
		if (StringUtils.isNotEmpty(summaryModel.getActivityIca())) {
			andClause += " AND cd.activity_ica ='" + summaryModel.getActivityIca() + "'";
		}
		return andClause;
	}
	/** 
	 * @param totalRecords .
	 * @param summaryModel .
	 * @param aggregateFields
	 * @param roleName
	 **/
	public List<Map<String, Object>> findAggregateRecords(SummaryModel summaryModel, List<String> aggregateFields, int totalRecords, String roleName) {

		String sqlQuery = buildSqlQuery(summaryModel,aggregateFields);
		log.info("DB query to group by summary details {}",sqlQuery);
		List<Map<String, Object>> queryForList = jdbcTemplate.queryForList(sqlQuery);

		List<String> fieldsList = summaryModel.getFieldsList();

		//get the group by columns and aggregate columns
		Map<String, List<String>> groupByAndAggregateColumns = getGroupByAndAggregateColumns(fieldsList,aggregateFields);

		List<Map<String, Object>> resultForList = new ArrayList<>();

		Map<String, Map<String, Object>> resultMap = new HashMap<>();

		for (Map<String, Object> dataMap : queryForList) {

			buildAggregateData(resultMap, dataMap,groupByAndAggregateColumns);
		}
		for (Entry<String, Map<String, Object>> map : resultMap.entrySet()) {
			resultForList.add(map.getValue());
		}

		log.info("Before Aggregate data size {} ",queryForList.size());
		log.info("After Aggregate data size {} ",resultForList.size());
		return resultForList;
	}
	/** This method builds db query which includes fieldsList from summary model and summary trace id as a part of group by columns */
	private String buildSqlQuery(SummaryModel summaryModel, List<String> aggregateFields) {
		StringBuilder sqlQuery = new StringBuilder("SELECT ");
		sqlQuery
		.append(buildSelectClause(summaryModel.getFieldsList()))
		.append(",SUMM_TRACE_ID ")
		.append("FROM bcx_owner.charge_detail cd WHERE cd.chrg_dt = to_date(' ")
		.append(summaryModel.getInvoiceDate()).append("','MM-DD-YYYY')")
		.append(buildAndClauseString(summaryModel))
		.append(" GROUP BY SUMM_TRACE_ID,")
		.append(buildGroupByClause(summaryModel.getFieldsList()));
		log.info("Group by query {}",sqlQuery);
		return sqlQuery.toString();
	}

	private String buildGroupByClause(List<String> fieldsList) {
		return String.join(COMMA, fieldsList).toString();
	}

	private String buildSelectClause(List<String> fieldsList) {
		return String.join(COMMA, fieldsList);

	}

	/** This method returns separate lists of aggregate and group by columns*/
	private Map<String, List<String>> getGroupByAndAggregateColumns(List<String> fieldsList, List<String> aggregateFields) {
		Map<String,List<String>> groupByAndAggregateColumns =new HashMap<>();
		List<String> aggregateColumnList=new ArrayList<>();
		List<String> groupByColumnList=new ArrayList<>();
		for (int i = 0; i < fieldsList.size(); i++) {
			if(aggregateFields.stream().anyMatch(fieldsList.get(i)::equalsIgnoreCase)){
				aggregateColumnList.add(fieldsList.get(i));
			}else {
				groupByColumnList.add(fieldsList.get(i));
			}
		}
		groupByAndAggregateColumns.put(AGGREGATE_COLUMNS, aggregateColumnList);
		groupByAndAggregateColumns.put(GROUP_BY_COLUMNS, groupByColumnList);
		return groupByAndAggregateColumns;
	}

	private void buildAggregateData(Map<String, Map<String, Object>> resultMap, Map<String, Object> dataMap, Map<String, List<String>> groupByAndAggregateColumns) {

		//generate the hash key based on the group by columns
		String hashKey = generateHashKey(groupByAndAggregateColumns.get(GROUP_BY_COLUMNS),dataMap);
		//
		List<String> aggregateColumnsList = groupByAndAggregateColumns.get(AGGREGATE_COLUMNS);
		String summTraceId  = (String) dataMap.get(SUMMARY_TRACE_ID);
		Double tranAmt  = 0.0;
		Double qtyNum  = 0.0;
		Double chrgAmtLoc  = 0.0;
		Double chrgAmtUsd  = 0.0;

		for (String aggregateColumnName : aggregateColumnsList) {

			Object aggregateColumn = dataMap.get(aggregateColumnName);
			if(aggregateColumn != null) {

				switch (aggregateColumnName) {
				case "TRAN_AMT":
					tranAmt  = ((BigDecimal) dataMap.get("TRAN_AMT")).doubleValue();
					break;

				case "QTY_NUM":
					qtyNum  = ((BigDecimal) dataMap.get("QTY_NUM")).doubleValue();
					break;
				case "CHRG_AMT_LOC":
					chrgAmtLoc  = ((BigDecimal) dataMap.get("CHRG_AMT_LOC")).doubleValue();
					break;
				case "CHRG_AMT_USD":
					chrgAmtUsd  = ((BigDecimal) dataMap.get("CHRG_AMT_USD")).doubleValue();
					break;

				default:
					break;
				}

			}

		}

		Map<String, Object> dumpData = resultMap.get(hashKey);

		if(dumpData == null) {
			resultMap.put(hashKey, dataMap);
		}else {
			Double previousTransAmount = 0.0;
			Double previousQtyNum = 0.0;
			Double previousChrgAmtLoc = 0.0;
			Double previousChrgAmtUsd = 0.0;
			for (String aggregateColumnName : aggregateColumnsList) {
				Object aggregateColumn = dataMap.get(aggregateColumnName);
				if(aggregateColumn != null) {
					switch (aggregateColumnName) {
					case "TRAN_AMT":
						try {
							previousTransAmount =  ((BigDecimal) dumpData.get("TRAN_AMT")).doubleValue();
						} catch (Exception e) {
						}
						dumpData.put("TRAN_AMT",BigDecimal.valueOf( tranAmt + previousTransAmount));
						break;
						
					case "QTY_NUM":
						try {
						previousQtyNum  = ((BigDecimal) dumpData.get("QTY_NUM")).doubleValue();
						}catch (Exception e) {
						}
						dumpData.put("QTY_NUM", BigDecimal.valueOf(qtyNum + previousQtyNum));
						break;
					case "CHRG_AMT_LOC":
						try {
							previousChrgAmtLoc  = ((BigDecimal) dumpData.get("CHRG_AMT_LOC")).doubleValue();
						} catch (Exception e) {
							e.printStackTrace();
						}
						dumpData.put("CHRG_AMT_LOC", BigDecimal.valueOf(chrgAmtLoc + previousChrgAmtLoc));
						break;
					case "CHRG_AMT_USD":
						try {
							previousChrgAmtUsd  = ((BigDecimal) dumpData.get("CHRG_AMT_USD")).doubleValue();
						} catch (Exception e) {
							e.printStackTrace();
						}
						dumpData.put("CHRG_AMT_USD", BigDecimal.valueOf(chrgAmtUsd + previousChrgAmtUsd));
						break;

					default:
						break;
					}
				}
			}

			String previousValue = (String) dumpData.get(SUMMARY_TRACE_ID);
			dumpData.put(SUMMARY_TRACE_ID, previousValue + ","+summTraceId);
		}
	}

	private String generateHashKey(List<String> groupByColumns, Map<String, Object> dataMap) {
		StringBuilder hashKey = new StringBuilder();
		for (int i = 0; i < groupByColumns.size(); i++) {
			hashKey.append(dataMap.get(groupByColumns.get(i)));
		}
		return hashKey.toString();
	}

	/*This method performs group by and aggregate by sql query */
	public List<Map<String, Object>> findAggregateRecordsBySQL(SummaryModel summaryModel, List<String> aggregateFields,
			int totalRecords, String roleName) {
		String sql="SELECT  INV_ICA,ACTIVITY_ICA,PRDCT_ID,sum(TRAN_AMT),count(SUMM_TRACE_ID),\r\n" + 
				"rtrim(xmlelement(\"e\", cast(collect(distinct SUMM_TRACE_ID || ',') as sys.odcivarchar2list))\r\n" + 
				".extract('//text()').getclobval(), ',') as ACTIVITY_ICA from BCX_OWNER.CHARGE_DETAIL where\r\n" + 
				"CHRG_DT ='27-OCT-19'\r\n" + 
				"and inv_ica = '11429'\r\n" + 
				"GROUP BY INV_ICA,ACTIVITY_ICA,PRDCT_ID";
		return jdbcTemplate.queryForList(sql);
	}
}
