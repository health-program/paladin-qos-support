package com.paladin.data.dynamic.handler;

import java.io.BufferedReader;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.ibatis.type.BaseTypeHandler;
import org.apache.ibatis.type.JdbcType;
import org.apache.ibatis.type.MappedTypes;

import oracle.sql.CLOB;

@MappedTypes({ CLOB.class })
public class OracleClobTypeHandler extends BaseTypeHandler<Object> {

	@Override
	public void setNonNullParameter(PreparedStatement ps, int i, Object parameter, JdbcType jdbcType) throws SQLException {
		ps.setObject(i, parameter);
	}

	@Override
	public Object getNullableResult(ResultSet rs, String columnName) throws SQLException {
		Object result = rs.getObject(columnName);
		return rs.wasNull() ? null : dealResult(result);
	}

	@Override
	public Object getNullableResult(ResultSet rs, int columnIndex) throws SQLException {
		Object result = rs.getObject(columnIndex);
		return rs.wasNull() ? null : dealResult(result);
	}

	@Override
	public Object getNullableResult(CallableStatement cs, int columnIndex) throws SQLException {
		Object result = cs.getObject(columnIndex);
		return cs.wasNull() ? null : dealResult(result);
	}

	/**
	 * 为了解决错误： 26-Sep-2018 14:21:06.634 WARNING [http-apr-8080-exec-6]
	 * org.springframework.web.servlet.mvc.support.DefaultHandlerExceptionResolver.handleHttpMessageNotWritable
	 * Failed to write HTTP message:
	 * org.springframework.http.converter.HttpMessageNotWritableException: Could not
	 * write JSON: No serializer found for class java.io.ByteArrayInputStream and no
	 * properties discovered to create BeanSerializer (to avoid exception, disable
	 * SerializationFeature.FAIL_ON_EMPTY_BEANS); nested exception is
	 * com.fasterxml.jackson.databind.exc.InvalidDefinitionException: No serializer
	 * found for class java.io.ByteArrayInputStream and no properties discovered to
	 * create BeanSerializer (to avoid exception, disable
	 * SerializationFeature.FAIL_ON_EMPTY_BEANS) (through reference chain:
	 * java.util.HashMap["pageData"]->java.util.ArrayList[0]->java.util.HashMap["UPDATE_TIME"]->oracle.sql.TIMESTAMP["stream"])
	 * 
	 * @param result
	 * @return
	 * @throws SQLException
	 */
	private Object dealResult(Object result) throws SQLException {
		if (result instanceof CLOB) {
			CLOB clob = (CLOB) result;
			Reader is = clob.getCharacterStream();
			StringBuilder sb = new StringBuilder();
			try (BufferedReader br = new BufferedReader(is)) {
				do {
					String s = br.readLine();
					if (s == null) {
						break;
					}
					sb.append(s);
				} while (true);
				return sb.toString();
			} catch (Exception e) {
				throw new SQLException(e);
			}
		}
		return result;
	}

}
