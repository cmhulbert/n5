/**
 * Copyright (c) 2017--2021, Stephan Saalfeld
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

import java.io.IOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;

import com.google.gson.Gson;
import com.google.gson.JsonElement;

/**
 * {@link N5Reader} implementation through {@link KeyValueAccess} with JSON
 * attributes parsed with {@link Gson}.
 *
 */
public interface GsonKeyValueN5Reader extends GsonN5Reader {

	KeyValueAccess getKeyValueAccess();

	default boolean groupExists(final String normalPath) {

		return getKeyValueAccess().isDirectory(absoluteGroupPath(normalPath));
	}

	@Override
	default boolean exists(final String pathName) {

		final String normalPath = N5URI.normalizeGroupPath(pathName);
		return groupExists(normalPath) || datasetExists(normalPath);
	}

	@Override
	default boolean datasetExists(final String pathName) throws N5Exception {

		// for n5, every dataset must be a group
		return getDatasetAttributes(pathName) != null;
	}

	@Override
	default boolean blockExists(final String dataset, final long[] blockPosition) {
		final String path = getURI().getPath();
		final String[] blockParts = new String[1 + 1 + blockPosition.length];
		blockParts[0] = path;
		blockParts[1] = dataset;
		for (int i = 0; i < blockPosition.length; i++) {
			blockParts[i+2] = Long.toString(blockPosition[i]);
		}
		final String blockPath = getKeyValueAccess().compose(blockParts);
		return getKeyValueAccess().exists(blockPath);
	}

	/**
	 * Reads or creates the attributes map of a group or dataset.
	 *
	 * @param pathName
	 *            group path
	 * @return the attribute
	 * @throws N5Exception if the attributes cannot be read
	 */
	@Override
	default JsonElement getAttributes(final String pathName) throws N5Exception {

		final String groupPath = N5URI.normalizeGroupPath(pathName);
		final String attributesPath = absoluteAttributesPath(groupPath);

		if (!getKeyValueAccess().isFile(attributesPath))
			return null;

		try (final LockedChannel lockedChannel = getKeyValueAccess().lockForReading(attributesPath)) {
			return GsonUtils.readAttributes(lockedChannel.newReader(), getGson());
		} catch (final IOException e) {
			throw new N5IOException("Failed to read attributes from dataset " + pathName, e);
		}

	}

	@Override
	default DataBlock<?> readBlock(
			final String pathName,
			final DatasetAttributes datasetAttributes,
			final long... gridPosition) throws N5Exception {

		final String path = absoluteDataBlockPath(N5URI.normalizeGroupPath(pathName), gridPosition);
		if (!getKeyValueAccess().isFile(path))
			return null;

		try (final LockedChannel lockedChannel = getKeyValueAccess().lockForReading(path)) {
			return DefaultBlockReader.readBlock(lockedChannel.newInputStream(), datasetAttributes, gridPosition);
		} catch (final IOException e) {
			throw new N5IOException(
					"Failed to read block " + Arrays.toString(gridPosition) + " from dataset " + path,
					e);
		}
	}

	@Override
	default String[] list(final String pathName) throws N5Exception {

		try {
			return getKeyValueAccess().listDirectories(absoluteGroupPath(pathName));
		} catch (final IOException e) {
			throw new N5IOException("Cannot list directories for group " + pathName, e);
		}
	}

	/**
	 * Constructs the path for a data block in a dataset at a given grid
	 * position.
	 * <p>
	 * The returned path is
	 *
	 * <pre>
	 * $basePath/datasetPathName/$gridPosition[0]/$gridPosition[1]/.../$gridPosition[n]
	 * </pre>
	 * <p>
	 * This is the file into which the data block will be stored.
	 *
	 * @param normalPath
	 *            normalized dataset path
	 * @param gridPosition to the target data block
	 * @return the absolute path to the data block ad gridPosition
	 */
	default String absoluteDataBlockPath(
			final String normalPath,
			final long... gridPosition) {

		final String[] components = new String[gridPosition.length + 2];
		components[0] = getURI().getPath();
		components[1] = normalPath;
		int i = 1;
		for (final long p : gridPosition)
			components[++i] = Long.toString(p);

		return getKeyValueAccess().compose(components);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the group or
	 * dataset.
	 *
	 * @param normalGroupPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the group
	 */
	default String absoluteGroupPath(final String normalGroupPath) {

		return getKeyValueAccess().compose(getURI(), normalGroupPath);
	}

	/**
	 * Constructs the absolute path (in terms of this store) for the attributes
	 * file of a group or dataset.
	 *
	 * @param normalPath
	 *            normalized group path without leading slash
	 * @return the absolute path to the attributes
	 */
	default String absoluteAttributesPath(final String normalPath) {

		return getKeyValueAccess().compose(getURI(), normalPath, N5KeyValueReader.ATTRIBUTES_JSON);
	}
}
