""" Full-frame colour event videos, cut from a pre-roll ring buffer of encoded H.264.

The capture pipeline feeds a side branch into an appsink. Every encoded access unit is kept in an
in-memory ring (a few tens of MB). When the extractor reports a detection (which happens ~10+ s after the
meteor, once the 256-frame block has been compressed and analysed), the session cut starts at the last
keyframe before the event, continues with live buffers until the event is over, and is remuxed (no
re-encode) into a single video file. Events continuing into the next FF block are merged into one file.
"""

from __future__ import print_function, division, absolute_import

import os
import threading
import time
from collections import deque

from RMS.Logger import getLogger
from RMS.Misc import mkdirP

try:
    import queue as queue_module
except ImportError:
    import Queue as queue_module

try:
    import gi
    gi.require_version('Gst', '1.0')
    from gi.repository import Gst
except (ImportError, ValueError):
    Gst = None


log = getLogger("rmslogger")


# Caps of the ring buffer appsink: byte-stream access units, SPS/PPS repeated on every keyframe
RING_CAPS = "video/x-h264,stream-format=byte-stream,alignment=au"

# Safety limits
MAX_RING_BYTES = 200*1024*1024
MAX_SESSION_SECONDS = 300.0

# Start the video slightly before the detected event
PRE_PAD_SECONDS = 1.0

# Extra time to wait for the next FF block's result when the event touches the end of a block
NEXT_BLOCK_GRACE_SECONDS = 30.0


def buildRingBranch(config, event_encoder=None, queue_size=100):
    """ Build the GStreamer tee branch that feeds the ring buffer appsink.

    Arguments:
        config: [Config]

    Keyword arguments:
        event_encoder: [str] Encoder description for raw sources. None for sources which are already
            H.264 (RTSP) - the stream is used as is, without re-encoding.
        queue_size: [int] Max number of buffers in the leaky queue in front of the branch.

    Return:
        [str] Pipeline description of the branch.
    """

    encoder = "" if event_encoder is None else "{:s} ! ".format(event_encoder)

    return (
        "t. ! queue leaky=downstream max-size-buffers={:d} max-size-bytes=0 max-size-time=0 ! "
        "{:s}h264parse config-interval=-1 ! {:s} ! "
        "appsink name=event_ring_sink emit-signals=true sync=false drop=false max-buffers=0"
        ).format(queue_size, encoder, RING_CAPS)


def buildEventEncoder(config, is_rpi4):
    """ Encoder used to feed the ring from raw (uncompressed) sources. One keyframe per second, so that
    events can be cut with ~1 s precision.
    """

    fps = max(int(round(config.fps)), 1)

    if is_rpi4:
        return (
            "v4l2h264enc extra-controls=\"controls,video_bitrate={:d},h264_i_frame_period={:d};\""
            ).format(int(config.raw_video_bitrate)*1000, fps)

    return (
        "videoconvert ! video/x-raw,format=I420 ! "
        "x264enc speed-preset=ultrafast tune=zerolatency bframes=0 key-int-max={:d} bitrate={:d}"
        ).format(fps, int(config.raw_video_bitrate))



class EventVideoManager(object):
    """ Keeps the pre-roll ring buffer and turns extractor detections into single video files. """

    def __init__(self, capture):
        """
        Arguments:
            capture: [BufferedCapture] Owner. Used for the pipeline timeline (start_timestamp and PTS
                correction) and the configuration.
        """

        self.capture = capture
        self.config = capture.config

        self.pipeline = None
        self.appsink = None
        self._handler_id = None

        self._lock = threading.Lock()

        # Ring entries: (timestamp, is_keyframe, pts, dts, duration, bytes)
        self._ring = deque()
        self._ring_bytes = 0
        self._caps = None

        self._session = None
        self._writers = []

        self.preroll = float(getattr(self.config, 'event_video_preroll', 60.0))
        self.tail = float(getattr(self.config, 'event_video_tail', 5.0))
        self.block_seconds = 256.0/float(self.config.fps)


    def bind(self, pipeline):
        """ Attach to the ring appsink of the pipeline. """

        self.pipeline = pipeline
        self.appsink = pipeline.get_by_name("event_ring_sink")

        if self.appsink is None:
            raise ValueError("Could not get event video ring appsink from pipeline")

        self._handler_id = self.appsink.connect("new-sample", self._onNewSample)


    def unbind(self):
        """ Detach from the pipeline, flush the open session (if any) and wait for the writers. """

        if (self.appsink is not None) and (self._handler_id is not None):
            try:
                self.appsink.disconnect(self._handler_id)
            except Exception as e:
                log.debug("EventVideoManager: could not disconnect ring appsink: %s", e)

        self._handler_id = None
        self.appsink = None
        self.pipeline = None

        with self._lock:
            self._finalizeLocked()
            self._ring.clear()
            self._ring_bytes = 0
            self._caps = None
            writers = list(self._writers)

        for writer in writers:
            writer.join(timeout=30)


    def _timestampOf(self, sample, buffer):
        """ Convert the PTS of a buffer to wall-clock time, on the same timeline as the frame
        timestamps of the capture loop. """

        pts = buffer.pts
        if pts == Gst.CLOCK_TIME_NONE:
            return None

        running_ns = pts
        segment = sample.get_segment()
        if segment is not None:
            converted = segment.to_running_time(Gst.Format.TIME, pts)
            if converted != Gst.CLOCK_TIME_NONE:
                running_ns = converted

        return self.capture.start_timestamp + (running_ns + self.capture.last_pts_correction_ns)/1e9


    def _onNewSample(self, appsink):
        """ Called from a GStreamer streaming thread for every encoded access unit. """

        sample = appsink.emit("pull-sample")
        if sample is None:
            return Gst.FlowReturn.OK

        try:
            buffer = sample.get_buffer()
            timestamp = self._timestampOf(sample, buffer)

            if timestamp is None:
                return Gst.FlowReturn.OK

            ok, map_info = buffer.map(Gst.MapFlags.READ)
            if not ok:
                return Gst.FlowReturn.OK

            try:
                data = bytes(map_info.data)
            finally:
                buffer.unmap(map_info)

            entry = (timestamp, not buffer.has_flags(Gst.BufferFlags.DELTA_UNIT), buffer.pts, buffer.dts,
                     buffer.duration, data)

            with self._lock:
                if self._caps is None:
                    self._caps = sample.get_caps().to_string()

                self._ring.append(entry)
                self._ring_bytes += len(data)

                # Trim by age and size
                while self._ring and ((timestamp - self._ring[0][0] > self.preroll) \
                        or (self._ring_bytes > MAX_RING_BYTES)):
                    self._ring_bytes -= len(self._ring.popleft()[5])

                if self._session is not None:
                    self._session['samples'].append(entry)
                    self._session['last_ts'] = timestamp

        except Exception as e:
            log.error("EventVideoManager: ring buffer error: %s", e)

        return Gst.FlowReturn.OK


    def addEvent(self, filename, start_time, end_time, touches_end):
        """ Register a detection reported by the extractor.

        Arguments:
            filename: [str] FF file name of the block the detection comes from.
            start_time, end_time: [float] Wall-clock time range of the event.
            touches_end: [bool] The event reaches the end of the FF block, i.e. it may continue in the
                next block and the next block's result should be awaited before the file is closed.
        """

        with self._lock:
            now = time.time()

            if self._session is None:
                cut = start_time - PRE_PAD_SECONDS
                samples = list(self._ring)

                # Start at the last keyframe at or before the cut time
                start_idx = None
                for i, entry in enumerate(samples):
                    if entry[1]:
                        if entry[0] <= cut:
                            start_idx = i
                        elif start_idx is None:
                            start_idx = i
                            break
                        else:
                            break

                if start_idx is None:
                    log.warning("[RMS Event Video] No keyframe in the ring buffer for %s, skipping", filename)
                    return

                if samples[start_idx][0] > cut + 2.0:
                    log.warning("[RMS Event Video] Ring buffer does not reach the event start (%.1f s late)"
                                " for %s. Increase event_video_preroll.", samples[start_idx][0] - cut, filename)

                self._session = {
                    'filename': filename,
                    'start_ts': start_time,
                    'end_ts': end_time,
                    'samples': samples[start_idx:],
                    'last_ts': samples[-1][0],
                    'hold_until': 0.0,
                    'opened': now,
                    }

                log.info("[RMS Event Video] Event session started for %s", filename)

            else:
                self._session['end_ts'] = max(self._session['end_ts'], end_time)
                log.info("[RMS Event Video] Event from %s merged into the session of %s", filename,
                         self._session['filename'])

            # If the event may continue, wait for the next block's detection result
            if touches_end:
                self._session['hold_until'] = now + self.block_seconds + NEXT_BLOCK_GRACE_SECONDS
            else:
                self._session['hold_until'] = 0.0


    def poll(self):
        """ Finalize the session once the event is over. Call regularly from the capture loop. """

        with self._lock:
            session = self._session
            if session is None:
                return

            now = time.time()
            too_long = (session['last_ts'] - session['start_ts']) > MAX_SESSION_SECONDS
            tail_captured = session['last_ts'] >= (session['end_ts'] + self.tail)

            if too_long or (tail_captured and (now >= session['hold_until'])):
                self._finalizeLocked()


    def _finalizeLocked(self):
        """ Hand the open session over to a writer thread. Lock must be held. """

        session, self._session = self._session, None
        if session is None:
            return

        end_ts = session['end_ts'] + self.tail
        samples = [entry for entry in session['samples'] if entry[0] <= end_ts]

        if len(samples) == 0:
            return

        output_path = self._buildOutputPath(session['filename'])

        self._writers = [w for w in self._writers if w.is_alive()]
        writer = threading.Thread(target=self._writeVideo, args=(samples, self._caps, output_path),
                                  name="EventVideoWriter")
        writer.daemon = True
        writer.start()
        self._writers.append(writer)


    def _buildOutputPath(self, filename):
        video_root = os.path.join(self.config.data_dir, self.config.video_dir)
        if not mkdirP(video_root):
            raise IOError("Could not create event video directory: {}".format(video_root))

        return os.path.join(video_root, "EV_{}.mp4".format(filename))


    @staticmethod
    def _writeVideo(samples, caps, output_path):
        """ Remux the collected access units into an mp4 file, without re-encoding. """

        try:
            pipeline = Gst.parse_launch(
                "appsrc name=src format=time is-live=false block=true ! h264parse ! mp4mux ! "
                "filesink location=\"{:s}\"".format(output_path))

            src = pipeline.get_by_name("src")
            src.set_property("caps", Gst.Caps.from_string(caps or RING_CAPS))

            pipeline.set_state(Gst.State.PLAYING)

            # Rebase the timeline to zero
            base = None
            for entry in samples:
                first = entry[3] if entry[3] != Gst.CLOCK_TIME_NONE else entry[2]
                if first != Gst.CLOCK_TIME_NONE:
                    base = first
                    break

            base = 0 if base is None else base

            for _, _, pts, dts, duration, data in samples:
                buf = Gst.Buffer.new_wrapped(data)

                if pts != Gst.CLOCK_TIME_NONE:
                    buf.pts = max(pts - base, 0)

                buf.dts = max(dts - base, 0) if dts != Gst.CLOCK_TIME_NONE else buf.pts

                if duration != Gst.CLOCK_TIME_NONE:
                    buf.duration = duration

                src.emit("push-buffer", buf)

            src.emit("end-of-stream")

            bus = pipeline.get_bus()
            msg = bus.timed_pop_filtered(60*Gst.SECOND, Gst.MessageType.EOS | Gst.MessageType.ERROR)

            if (msg is not None) and (msg.type == Gst.MessageType.ERROR):
                err, _ = msg.parse_error()
                log.error("[RMS Event Video] Muxing failed: %s", err)
            elif msg is None:
                log.error("[RMS Event Video] Muxing timed out: %s", output_path)
            else:
                log.info("[RMS Event Video] Saved: %s", output_path)

            pipeline.set_state(Gst.State.NULL)

        except Exception as e:
            log.error("[RMS Event Video] Could not write %s: %s", output_path, e)


def processEventVideoMessages(capture):
    """ Drain extractor messages and let the manager finalize finished sessions. """

    manager = capture.event_video_manager

    if (capture.event_video_queue is None) or (manager is None):
        return

    while True:
        try:
            message = capture.event_video_queue.get_nowait()
        except queue_module.Empty:
            break

        if not isinstance(message, dict) or (message.get('command') != 'meteor_detected'):
            continue

        try:
            manager.addEvent(message['filename'], float(message['start_time']), float(message['end_time']),
                             bool(message.get('touches_end', False)))
        except (KeyError, TypeError, ValueError) as e:
            log.warning("[RMS Event Video] Malformed message %r: %s", message, e)

    manager.poll()
