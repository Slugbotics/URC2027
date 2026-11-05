import cv2
import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.duration import Duration
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup

CAMERA_IDS = [0]
PORTS = [5000]
LAPTOP_IP = '192.168.1.2'
WIDTH = 640
HEIGHT = 480

class CameraStreamer (Node):
    def __init__(self):
        super().__init__('camera_streamer')

        self.pipelines = [(
            "appsrc ! "
            "videoconvert ! "
            "video/x-raw,format=I420 ! "
            "x264enc "
            "bitrate=2000 "
            "speed-preset=ultrafast "
            "tune=zerolatency "
            "key-int-max=50 "
            "bframes=0 "
            "b-adapt=false "
            "threads=4 ! "
            "h264parse ! "
            "rtph264pay config-interval=1 pt=96 mtu=1200 ! "
            f"udpsink host={LAPTOP_IP} port={port} "
            "sync=false async=false"
        ) for port in PORTS]

        self.camera_timers = [self.create_timer(0.02, lambda i=i: self.send_image(i), MutuallyExclusiveCallbackGroup()) for i in range(len(CAMERA_IDS))]

        self.last_reconnect_times = [self.get_clock().now() - Duration(seconds=100) for _ in CAMERA_IDS]

        self.caps = [None for _ in CAMERA_IDS]
        self.writers = [None for _ in CAMERA_IDS]

        for i in range(len(CAMERA_IDS)):
            self.reconnect(i)

    def reconnect(self, index):
        if (self.get_clock().now() - self.last_reconnect_times[index]).nanoseconds < 1e9:
            return
        self.last_reconnect_times[index] = self.get_clock().now()
        if self.caps[index] is not None:
            self.caps[index].release()
        if self.writers[index] is not None:
            self.writers[index].release()
        self.caps[index] = cv2.VideoCapture(CAMERA_IDS[index])
        self.caps[index].set(cv2.CAP_PROP_BUFFERSIZE, 1)
        self.writers[index] = cv2.VideoWriter(self.pipelines[index], cv2.CAP_GSTREAMER, 0, 50, (WIDTH, HEIGHT), True)
    
    def send_image(self, index):
        cap = self.caps[index]
        writer = self.writers[index]
        if cap is None or not cap.isOpened() or writer is None or not writer.isOpened():
            self.get_logger().error(f'Camera or writer is not opened (index={index})', throttle_duration_sec=5.0)
            self.reconnect(index)
            return
        
        ret, frame = cap.read()
        if not ret:
            self.get_logger().error(f'Failed to read frame (index={index})', throttle_duration_sec=5.0)
            self.reconnect(index)
            return

        frame = cv2.resize(frame, (WIDTH, HEIGHT))
        writer.write(frame)

    def destroy_node(self):
        for cap in self.caps:
            if cap is not None:
                cap.release()
        for writer in self.writers:
            if writer is not None:
                writer.release()
        super().destroy_node()

def main():
    rclpy.init()
    node = CameraStreamer()
    executor = MultiThreadedExecutor()
    executor.add_node(node)
    try:
        executor.spin()
    except KeyboardInterrupt:
        print()
    node.destroy_node()
    if rclpy.ok():
        rclpy.shutdown()

if __name__ == '__main__':
    main()