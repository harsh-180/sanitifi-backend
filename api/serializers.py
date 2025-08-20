from rest_framework import serializers
from .models import User, EDAPlot, EDAFormat

class UserSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True)

    class Meta:
        model = User
        fields = ('id', 'username', 'email', 'password')
    
    def create(self, validated_data):
        user = User.objects.create_user(
            username=validated_data['username'],
            email=validated_data['email'],
            password=validated_data['password']
        )
        return user

class EDAPlotSerializer(serializers.ModelSerializer):
    # Computed fields for better compatibility
    y_axes = serializers.SerializerMethodField()
    x_axes = serializers.SerializerMethodField()
    chart_type = serializers.SerializerMethodField()
    aggregation_method = serializers.SerializerMethodField()
    date_grouping = serializers.SerializerMethodField()
    
    class Meta:
        model = EDAPlot
        fields = '__all__'
        read_only_fields = ('id', 'created_at', 'updated_at')
    
    def get_y_axes(self, obj):
        """Get Y-axes configuration"""
        return obj.get_y_axes()
    
    def get_x_axes(self, obj):
        """Get X-axes configuration"""
        return obj.get_x_axes()
    
    def get_chart_type(self, obj):
        """Get chart type"""
        return obj.get_chart_type()
    
    def get_aggregation_method(self, obj):
        """Get aggregation method"""
        return obj.get_aggregation_method()
    
    def get_date_grouping(self, obj):
        """Get date grouping configuration"""
        return obj.get_date_grouping()
    
    def validate_plot_name(self, value):
        """
        Ensure plot name is unique for the user and project combination
        """
        user = self.context.get('user')
        project = self.context.get('project')
        instance = self.instance
        
        if user and project:
            existing_plot = EDAPlot.objects.filter(
                user=user,
                project=project,
                plot_name=value
            )
            if instance:
                existing_plot = existing_plot.exclude(id=instance.id)
            
            if existing_plot.exists():
                raise serializers.ValidationError(
                    f"A plot with the name '{value}' already exists for this project."
                )
        
        return value

class EDAFormatSerializer(serializers.ModelSerializer):
    class Meta:
        model = EDAFormat
        fields = '__all__'
        read_only_fields = ('id', 'created_at', 'updated_at', 'usage_count', 'last_used')
    
    def validate_format_name(self, value):
        """
        Ensure format name is unique for the user
        """
        user = self.context.get('user')
        instance = self.instance
        
        if user:
            existing_format = EDAFormat.objects.filter(
                user=user,
                format_name=value
            )
            if instance:
                existing_format = existing_format.exclude(id=instance.id)
            
            if existing_format.exists():
                raise serializers.ValidationError(
                    f"A format with the name '{value}' already exists."
                )
        
        return value
    
    def validate_format_config(self, value):
        """
        Validate format configuration structure
        """
        if not isinstance(value, dict):
            raise serializers.ValidationError("Format configuration must be a dictionary")
        
        # Check for required keys in format_config
        required_keys = ['plots', 'layout']
        for key in required_keys:
            if key not in value:
                raise serializers.ValidationError(f"Format configuration must contain '{key}' key")
        
        return value
    
    def validate_required_columns(self, value):
        """
        Validate required columns list
        """
        if not isinstance(value, list):
            raise serializers.ValidationError("Required columns must be a list")
        
        if len(value) == 0:
            raise serializers.ValidationError("At least one required column must be specified")
        
        return value