package org.sfm.jdbc.impl;

import org.sfm.jdbc.JdbcMapper;
import org.sfm.map.*;
import org.sfm.map.impl.MapperImpl;
import org.sfm.reflect.Instantiator;
import org.sfm.utils.RowHandler;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
//IFJAVA8_START
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
//IFJAVA8_END


public final class JdbcMapperImpl<T> extends MapperImpl<ResultSet, T> implements JdbcMapper<T> {

	
	private final RowHandlerErrorHandler errorHandler; 
	
	public JdbcMapperImpl(final FieldMapper<ResultSet, T>[] fieldMappers,
                          final FieldMapper<ResultSet, T>[] constructorMappers,
                          final Instantiator<ResultSet, T> instantiator,
                          final RowHandlerErrorHandler errorHandler,
                          final MappingContextFactory<ResultSet> mappingContextFactory) {
		super(fieldMappers, constructorMappers, instantiator, mappingContextFactory);
		this.errorHandler = errorHandler;
	}

    @Override
    public final <H extends RowHandler<? super T>> H forEach(final ResultSet rs, final H handler)
            throws SQLException, MappingException {
        return JdbcMapperHelper.forEach(rs, handler, newMappingContext(rs), this, errorHandler);
    }

	@Override
    @Deprecated
	public Iterator<T> iterate(ResultSet rs) throws SQLException,
			MappingException {
		return new ResultSetIterator<T>(rs, this, newMappingContext(rs));
	}

    @Override
    @SuppressWarnings("deprecation")
    public Iterator<T> iterator(ResultSet rs) throws SQLException,
			MappingException {
		return iterate(rs);
	}

	//IFJAVA8_START
	@Override
	public Stream<T> stream(ResultSet rs) throws SQLException, MappingException {
		return StreamSupport.stream(new ResultSetSpliterator<T>(rs, this, newMappingContext(rs)), false);
	}
	//IFJAVA8_END
}
