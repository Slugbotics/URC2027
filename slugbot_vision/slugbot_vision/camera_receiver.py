import cv2
import rclpy
from rclpy.node import Node
from rclpy.executors import MultiThreadedExecutor
from rclpy.duration import Duration
from rclpy.callback_groups import MutuallyExclusiveCallbackGroup
from sensor_msgs.msg import Image
from cv_bridge import CvBridge

PORTS = [5000, 5001]
WIDTH = 640
HEIGHT = 480

class CameraReceiver (Node):
    def __init__(self):
        super().__init__('camera_receiver')
        self.bridge = CvBridge()

        self.pipelines = [(
            f"udpsrc port={port} "
            "caps=\"application/x-rtp,media=(string)video,"
            "clock-rate=(int)90000,encoding-name=(string)H264,payload=(int)96\" ! "
            "rtpjitterbuffer latency=50 ! "
            "rtph264depay ! "
            "h264parse ! "
            "avdec_h264 ! "
            "videoconvert ! "
            "video/x-raw,format=BGR ! "
            "appsink max-buffers=1 drop=true sync=false"
        ) for port in PORTS]

        self.image_publishers = [self.create_publisher(Image, f'camera_{i}', 10) for i in range(len(PORTS))]

        self.camera_timers = [self.create_timer(0.02, lambda i=i: self.receive_image(i), MutuallyExclusiveCallbackGroup()) for i in range(len(PORTS))]

        self.last_reconnect_times = [self.get_clock().now() - Duration(seconds=100) for _ in PORTS]

        self.caps = [None for _ in PORTS]

        for i in range(len(PORTS)):
            self.reconnect(i)

    def reconnect(self, index):
        if (self.get_clock().now() - self.last_reconnect_times[index]).nanoseconds < 1e9:
            return
        self.last_reconnect_times[index] = self.get_clock().now()
        if self.caps[index] is not None:
            self.caps[index].release()
        self.caps[index] = cv2.VideoCapture(self.pipelines[index], cv2.CAP_GSTREAMER)
    
    def receive_image(self, index):
        cap = self.caps[index]
        if cap is None or not cap.isOpened():
            self.get_logger().error(f'Camera stream is not opened (index={index})', throttle_duration_sec=5.0)
            self.reconnect(index)
            return
        
        ret, frame = cap.read()
        if not ret:
            self.get_logger().error(f'Failed to read frame (index={index})', throttle_duration_sec=5.0)
            self.reconnect(index)
            return

        self.image_publishers[index].publish(self.bridge.cv2_to_imgmsg(frame, encoding='bgr8'))
        cv2.imshow(f'Camera {index}', frame)
        cv2.waitKey(1)

    def destroy_node(self):
        for cap in self.caps:
            if cap is not None:
                cap.release()
        super().destroy_node()

def main():
    rclpy.init()
    node = CameraReceiver()
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