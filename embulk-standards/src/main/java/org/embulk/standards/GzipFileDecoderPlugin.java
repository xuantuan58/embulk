package org.embulk.standards;

import org.embulk.config.ConfigInject;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskSource;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.DecoderPlugin;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInput;
import org.embulk.spi.util.FileInputInputStream;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamFileInput.InputStreamWithHints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

public class GzipFileDecoderPlugin implements DecoderPlugin {
    public interface PluginTask extends Task {
        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    @Override
    public void transaction(ConfigSource config, DecoderPlugin.Control control) {
        PluginTask task = config.loadConfig(PluginTask.class);
        control.run(task.dump());
    }

    @Override
    public FileInput open(TaskSource taskSource, FileInput fileInput) {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        final FileInputInputStream files = new FileInputInputStream(fileInput);
        return new InputStreamFileInput(
                task.getBufferAllocator(),
                new InputStreamFileInput.Provider() {
                    // Implement openNextWithHints() instead of openNext() to show file name at parser plugin loaded by FileInputPlugin
                    // Because when using decoder, parser plugin can't get file name.
                    @Override
                    public InputStreamWithHints openNextWithHints() throws IOException {
                        if (!files.nextFile()) {
                            return null;
                        }
                        int bufferSize = 8 * 1024;
                        GZIPInputStream inputStream = Exec.isPreview() ? new PreviewGzipInputStream(files, bufferSize)
                                : new GZIPInputStream(files, bufferSize);

                        return new InputStreamWithHints(
                                inputStream,
                                fileInput.hintOfCurrentInputFileNameForLogging().orElse(null)
                        );
                    }

                    @Override
                    public void close() throws IOException {
                        files.close();
                    }
                });
    }

    public static class PreviewGzipInputStream extends GZIPInputStream {
        public PreviewGzipInputStream(FileInputInputStream files, int size) throws IOException {
            super(files, size);
        }

        /**
         * EOFException is ignored in Preview because Preview aborts reading the entire GZipped stream,
         * and it can cause an unexpected end of ZLIB input stream if the number sample rows (default is 15) are not fulfilled.
         */
        @Override
        public int read(byte[] buf, int off, int len) throws IOException
        {
            try {
                return super.read(buf, off, len);
            } catch (EOFException ex) {
                logger.warn("Abort reading GZipped stream. All preview_sample_buffer_bytes have been read");
                return -1;
            }
        }
    }

    private static final Logger logger = LoggerFactory.getLogger(PreviewGzipInputStream.class);
}
