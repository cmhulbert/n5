package org.janelia.saalfeldlab.n5.shard;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;

import org.janelia.saalfeldlab.n5.DataBlock;
import org.janelia.saalfeldlab.n5.DatasetAttributes;
import org.janelia.saalfeldlab.n5.DefaultBlockReader;
import org.janelia.saalfeldlab.n5.KeyValueAccess;
import org.janelia.saalfeldlab.n5.LockedChannel;
import org.janelia.saalfeldlab.n5.N5Exception;
import org.janelia.saalfeldlab.n5.N5Exception.N5IOException;
import org.janelia.saalfeldlab.n5.ShardedDatasetAttributes;

public class VirtualShard<T> extends AbstractShard<T> {

	private KeyValueAccess keyValueAccess;
	private String path;

	public VirtualShard(final ShardedDatasetAttributes datasetAttributes, long[] gridPosition,
			final KeyValueAccess keyValueAccess, final String path) {

		super(datasetAttributes, gridPosition, null);
		this.keyValueAccess = keyValueAccess;
		this.path = path;
	}

	@SuppressWarnings("unchecked")
	@Override
	public DataBlock<T> getBlock(long... blockGridPosition) {

		final long[] relativePosition = getBlockPosition(blockGridPosition);
		if (relativePosition == null)
			throw new N5IOException("Attempted to read a block from the wrong shard.");

		final ShardIndex idx = getIndex();
		final long startByte = idx.getOffset(relativePosition);
		final long endByte = startByte + idx.getNumBytes(relativePosition);
		try (final LockedChannel lockedChannel = keyValueAccess.lockForReading(path, startByte, endByte)) {

			// TODO add codecs, generalize to use any BlockReader
			final DataBlock<T> dataBlock = (DataBlock<T>)datasetAttributes.getDataType().createDataBlock(
					datasetAttributes.getBlockSize(),
					blockGridPosition,
					numBlockElements(datasetAttributes));

			DefaultBlockReader.readFromStream(dataBlock, lockedChannel.newInputStream());
			return dataBlock;

		} catch (final N5Exception.N5NoSuchKeyException e) {
			return null;
		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}
	}

	@Override
	public void writeBlock(final DataBlock<T> block) {

		final long[] relativePosition = getBlockPosition(block.getGridPosition());
		if (relativePosition == null)
			throw new N5IOException("Attempted to write block in the wrong shard.");

		final ShardIndex idx = getIndex();
		final long startByte = idx.getOffset(relativePosition);
		final long endByte = startByte + idx.getNumBytes(relativePosition);

		// TODO this assumes that the block exists in the shard and
		// that the available space is sufficient. Should generalize

		// // A starting point:
		// if (!idx.exists(block.getGridPosition())) {
		//
		// }
		try (final LockedChannel lockedChannel = keyValueAccess.lockForWriting(path, startByte, endByte)) {

			// TODO codecs
			datasetAttributes.getCompression().getWriter().write(block, lockedChannel.newOutputStream());

			// TODO update index when we know how many bytes were written

		} catch (final IOException | UncheckedIOException e) {
			throw new N5IOException("Failed to read block from " + path, e);
		}

	}

	@Override
	public void writeShard() {

	}

	private static int numBlockElements(DatasetAttributes datasetAttributes) {

		return Arrays.stream(datasetAttributes.getBlockSize()).reduce(1, (x, y) -> x * y);
	}

	@Override
	public ShardIndex getIndex() {

		try {
			return ShardIndex.read(keyValueAccess, path, datasetAttributes);
		} catch (final IOException e) {
			throw new N5IOException("Failed to read index at " + path, e);
		}
	}

}
