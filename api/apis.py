from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

# Write new API views here

class ExampleNewAPI(APIView):
    def get(self, request):
        return Response({'message': 'This is a new API!'}, status=status.HTTP_200_OK) 