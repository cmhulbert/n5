/**
 * Copyright (c) 2017, Stephan Saalfeld
 * All rights reserved.
 * <p>
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * <p>
 * 1. Redistributions of source code must retain the above copyright notice,
 * this list of conditions and the following disclaimer.
 * 2. Redistributions in binary form must reproduce the above copyright notice,
 * this list of conditions and the following disclaimer in the documentation
 * and/or other materials provided with the distribution.
 * <p>
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
package org.janelia.saalfeldlab.n5;

import org.janelia.saalfeldlab.n5.codec.Codec.ArrayCodec;
import org.janelia.saalfeldlab.n5.readdata.ReadData;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

/**
 * Default implementation of {@link BlockReader}.
 *
 * @author Stephan Saalfeld
 * @author Igor Pisarev
 */
public interface DefaultBlockReader extends BlockReader {

	InputStream getInputStream(final InputStream in) throws IOException;

	@Override
	default <T, B extends DataBlock<T>> void read(
			final B dataBlock,
			final InputStream in) throws IOException {

		// do not try with this input stream because subsequent block reads may happen if the stream points to a shard
		final InputStream inflater = getInputStream(in);
		readFromStream(dataBlock, inflater);
	}

	/**
	 * Reads a {@link DataBlock} from an {@link InputStream}.
	 *
	 * @param in
	 *            the input stream
	 * @param datasetAttributes
	 *            the dataset attributes
	 * @param gridPosition
	 *            the grid position
	 * @return the block
	 * @throws IOException
	 *             the exception
	 */
	static DataBlock<?> readBlock(
			final InputStream in,
			final DatasetAttributes datasetAttributes,
			final long[] gridPosition) throws IOException {

		final ArrayCodec<?> codec = datasetAttributes.getArrayCodec();
		return codec.decode(ReadData.from(in), gridPosition);
	}

	static <T, B extends DataBlock<T>> void readFromStream(final B dataBlock, final InputStream in) throws IOException {

		final ByteBuffer buffer = dataBlock.toByteBuffer();
		final DataInputStream dis = new DataInputStream(in);
		dis.readFully(buffer.array());
		dataBlock.readData(buffer);
	}

}