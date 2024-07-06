import "dart:async";
import "package:flutter/material.dart";
import "package:flutter_device_name/flutter_device_name.dart";
import "package:geolocator/geolocator.dart";
import "package:get/get.dart";
import "package:google_maps_flutter/google_maps_flutter.dart";
import "package:location_tracker/res/constants.dart";
import "package:location_tracker/res/wamp_util.dart";
import "package:permission_handler/permission_handler.dart";

class LocationController extends GetxController {
  Rx<Position?> currentPosition = Rx<Position?>(null);
  RxList<LatLng> routeCoordinates = RxList<LatLng>();
  RxMap<String, Marker> markers = <String, Marker>{}.obs;
  RxMap<String, Polyline> polylines = <String, Polyline>{}.obs;
  GoogleMapController? mapController;
  StreamSubscription<Position>? _positionStreamSubscription;
  Rx<DateTime> lastPublishTime = DateTime.now().obs;
  String deviceName = "";

  Future<void> mobileModelName() async {
    final plugin = DeviceName();
    deviceName = (await plugin.getName())!;
    await _publish(30.19182, 71.44313, deviceName);
  }

  @override
  Future<void> onInit() async {
    super.onInit();
    await _checkLocationPermission();
    await _subscribe();
  }

  @override
  Future<void> onClose() async {
    await _positionStreamSubscription?.cancel();
    super.onClose();
  }

  Future<void> _checkLocationPermission() async {
    await mobileModelName();
    PermissionStatus permissionStatus = await Permission.location.status;
    if (permissionStatus.isGranted) {
      _startLocationUpdates();
    } else {
      PermissionStatus permissionResult = await Permission.location.request();
      if (permissionResult.isGranted) {
        _startLocationUpdates();
        Get.snackbar("Location", "Permission Granted Successfully");
      } else if (permissionResult.isDenied) {
        Get.snackbar("Location", "Permission is Denied");
      } else if (permissionResult.isPermanentlyDenied) {
        Get.snackbar(
          "Location",
          "Permission is Denied Permanently. Please Allow Permission to run the app without any issue",
        );
        await openAppSettings();
      }
      return;
    }
  }

  void _startLocationUpdates() {
    LocationSettings locationSettings = const LocationSettings(accuracy: LocationAccuracy.high, distanceFilter: 10);
    _positionStreamSubscription =
        Geolocator.getPositionStream(locationSettings: locationSettings).listen((Position position) async {
      currentPosition.value = position;
      routeCoordinates.add(LatLng(position.latitude, position.longitude));
      _updateMarker();
      _updatePolyline();
      await _moveCamera();
      await _refreshPublish(position.latitude, position.longitude);
    });
  }

  void _updateMarker() {
    if (currentPosition.value != null) {
      markers[deviceName] = Marker(
        markerId: MarkerId(deviceName),
        position: LatLng(
          currentPosition.value!.latitude,
          currentPosition.value!.longitude,
        ),
        infoWindow: InfoWindow(
          title: deviceName,
          snippet: "${currentPosition.value!.latitude}, ${currentPosition.value!.longitude}",
        ),
        icon: BitmapDescriptor.defaultMarkerWithHue(BitmapDescriptor.hueBlue),
      );
      update();
    }
  }

  void _updatePolyline() {
    if (routeCoordinates.length > 1) {
      polylines[deviceName] = Polyline(
        polylineId: PolylineId(deviceName),
        points: routeCoordinates.toList(),
        color: Colors.blue,
        width: 5,
      );
      update();
    }
  }

  Future<void> _moveCamera() async {
    if (currentPosition.value != null && mapController != null) {
      await mapController!.animateCamera(
        CameraUpdate.newLatLng(
          LatLng(currentPosition.value!.latitude, currentPosition.value!.longitude),
        ),
      );
    }
  }

  Future<void> onMapCreated(GoogleMapController controller) async {
    mapController = controller;
    await _moveCamera();
  }

  Future<void> _publish(double latitude, double longitude, String name) async {
    try {
      var session = await connect(
        "ws://192.168.0.124:8080/ws",
        "realm1",
        jsonSerializer,
      );

      await session.publish(
        "io.xconn.location",
        args: [latitude, longitude, name],
        kwargs: {},
      );
    } on Exception catch (e) {
      return Future.error(e);
    }
  }

  Future<void> _refreshPublish(double latitude, double longitude) async {
    final now = DateTime.now();
    if (now.difference(lastPublishTime.value).inSeconds >= 5) {
      lastPublishTime.value = now;
      await _publish(latitude, longitude, deviceName);
    }
  }

  Future<void> _subscribe() async {
    try {
      var session = await connect(
        "ws://192.168.0.124:8080/ws",
        "realm1",
        jsonSerializer,
      );

      await session.subscribe(
        "io.xconn.location",
        (event) {
          List<dynamic> args = event.args;
          String name = args[2];
          double latitude;
          double longitude;
          if (args[0] is String) {
            latitude = double.parse(args[0]);
          } else if (args[0] is double) {
            latitude = args[0];
          } else {
            throw Exception("Unexpected type for latitude: ${args.runtimeType}");
          }

          if (args[1] is String) {
            longitude = double.parse(args[1]);
          } else if (args[1] is double) {
            longitude = args[1];
          } else {
            throw Exception("Unexpected type for longitude: ${args.runtimeType}");
          }

          updateOtherLocation(name, latitude, longitude);
        },
      );
    } on Exception catch (error) {
      Get.snackbar("Error", "Error connecting or subscribing: $error");
    }
  }

  void updateOtherLocation(String name, double latitude, double longitude) {
    var deviceRouteCoordinates = (polylines[name]?.points.toList() ?? <LatLng>[])..add(LatLng(latitude, longitude));
    polylines[name] = Polyline(
      polylineId: PolylineId(name),
      points: deviceRouteCoordinates,
      color: Colors.red,
      width: 5,
    );

    markers[name] = Marker(
      markerId: MarkerId(name),
      position: LatLng(latitude, longitude),
      infoWindow: InfoWindow(
        title: name,
        snippet: "$latitude, $longitude",
      ),
      icon: BitmapDescriptor.defaultMarkerWithHue(BitmapDescriptor.hueRed),
    );

    update();
  }
}
