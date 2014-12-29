package org.sfm.jdbc;

import static org.junit.Assert.*;

import java.lang.reflect.Type;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.sfm.beans.DbObject;
import org.sfm.jdbc.impl.JdbcMapperImpl;
import org.sfm.map.MapperBuildingException;
import org.sfm.map.MappingException;
import org.sfm.map.RowHandlerErrorHandler;
import org.sfm.map.impl.FieldMapper;
import org.sfm.reflect.Getter;
import org.sfm.reflect.Instantiator;
import org.sfm.reflect.InstantiatorFactory;
import org.sfm.reflect.ReflectionService;
import org.sfm.reflect.asm.AsmFactory;
import org.sfm.reflect.ConstructorDefinition;
import org.sfm.reflect.ConstructorParameter;

public class ResultSetMapperBuilderImplTest {

	@Test
	public void testAddFieldMapper() {
		
		JdbcMapperBuilder<DbObject> builder = new JdbcMapperBuilder<DbObject>(DbObject.class);
		
		JdbcMapper<DbObject> mapper = builder.addMapper(new FieldMapper<ResultSet, DbObject>() {
			
			@Override
			public void map(ResultSet source, DbObject target) throws MappingException {
				target.setId(33);
			}
		}).mapper();
		
		assertEquals(33, mapper.map(null).getId());
	}

	
	@Test
	public void testAsmFailureOnJdbcMapper() {
		
		JdbcMapperBuilder<DbObject> builder = new JdbcMapperBuilder<DbObject>(DbObject.class, new ReflectionService(true, true, new AsmFactory(Thread.currentThread().getContextClassLoader()) {
			@Override
			public <T> JdbcMapper<T> createJdbcMapper(
					FieldMapper<ResultSet, T>[] mappers,
					Instantiator<ResultSet, T> instantiator,
					Class<T> target, RowHandlerErrorHandler errorHandler)
					throws Exception {
				throw new UnsupportedOperationException();
			}

		}));
		
		assertTrue(builder.mapper() instanceof JdbcMapperImpl);
	}
	
	public boolean asmPresent = true;
	@Test
	public void testAsmFailureOnInstantiator() {
		JdbcMapperBuilder<DbObject> builder = new JdbcMapperBuilder<DbObject>(DbObject.class, new ReflectionService(true, true, null) {
			@Override
			public InstantiatorFactory getInstantiatorFactory() {
				return new InstantiatorFactory(null) {
					@Override
					public <S, T> Instantiator<S, T> getInstantiator(Type target,
							Class<?> source,
							List<ConstructorDefinition<T>> constructors,
							Map<ConstructorParameter, Getter<S, ?>> injections, boolean useAsm)
							throws NoSuchMethodException, SecurityException {
						throw new UnsupportedOperationException();
					}
					
				};
			}
			
			public boolean isAsmPresent() {
				return asmPresent;
			}
		});
		
		try {
			builder.mapper();
			fail("Expected exception");
		} catch(MapperBuildingException e) {
		}

		asmPresent = false;
		try {
			builder.mapper();
			fail("Expected exception");
		} catch(MapperBuildingException e) {
		}
}


}
