package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {

    if (queryRectangle == null || queryRectangle.isEmpty() || pointString == null || pointString.isEmpty())
      return false

    // Split the points string to get X-axis and Y-axis values.
	  var pointStringSplit = pointString.split(",")
    var queryRectangleSplit = queryRectangle.split(",")

    // Convert the string values to double for the point.
    var point_x = pointStringSplit(0).toDouble
    var point_y = pointStringSplit(1).toDouble

    // Convert the string values to double for the rectangle.
    var rect_x1 = queryRectangleSplit(0).toDouble
    var rect_y1 = queryRectangleSplit(1).toDouble
    var rect_x2 = queryRectangleSplit(2).toDouble
    var rect_y2 = queryRectangleSplit(3).toDouble

    // Assuming that the rectangle is parallel to X and Y axis.
    // The given points of the rectangle is assumed as the endpoints
    // of a diagonal. Therefore the points with the lowest value will
    // be the lowest point and the points with the highest value will
    // be the highest point.
    if(rect_x1 > rect_x2) {
      var temp = rect_x1
      rect_x1 = rect_x2
      rect_x2 = temp
    }

    if(rect_y1 > rect_y2) {
      var temp = rect_y1
      rect_y1 = rect_y2
      rect_y2 = temp
    }

    // Check if the point is inside the rectangle. It also includes points on
    // the boundary. If it is there, then return true.
    if(point_x >= rect_x1 && point_y >= rect_y1 && point_x <= rect_x2 && point_y <= rect_y2) {
      return true
    }

    // If the point is outside the boundary, then return false.
    return false

  }
}
